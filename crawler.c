#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/timerfd.h>
#include <sys/types.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <time.h>
#include <unistd.h>
#include <pthread.h>
#include <netdb.h>
#include <time.h>
#include <netinet/in.h>
#include <mysql.h> 
#include <mysqld_error.h>
#include <curl/curl.h>
#include <ctype.h>

#define MSG_OUT stdout
#define DEFAULT_QUEUE_LENGTH 10000
#define mycase(code) \
        case code: s = __STRING(code)

#define MAX_CONNECTIONS 1024
#define ALL_ROWS_FETCHED 0
#define SELECT_DONE 1
#define FETCHED_RESULT 2
#define FETCHED_ROW 3
#define DELETE_DONE 4

#define READY 0
#define BUSY 1

int debug = 0;

/* Global information, common to all connections */
typedef struct _GlobalInfo
{
	int epfd;    /* epoll filedescriptor */
	int tfd;     /* timer filedescriptor */
	CURLM *multi;
	int still_running;
	pthread_mutex_t lock;
	int concurrent_connections;
	pthread_mutex_t parsed_lock;
	int parsed_sites;
	int transfers;
} GlobalInfo;

int new_body_conn(char *url, GlobalInfo *g);

/* Information associated with a specific easy handle */
typedef struct _ConnInfo
{
	CURL *easy;
	char *url;
	GlobalInfo *global;
	char error[CURL_ERROR_SIZE];
	size_t size;
	char *data;
} ConnInfo;

/* Information associated with a specific socket */
typedef struct _SockInfo
{
	curl_socket_t sockfd;
	CURL *easy;
	int action;
	long timeout;
	GlobalInfo *global;
} SockInfo;

typedef struct sql_node
{
	char sql[8192];
	struct sql_node *next;
} SqlNode;

SqlNode *sql_head = NULL;
SqlNode *sql_current;

typedef struct mysql_node
{
	MYSQL *mysql_conn;
	enum net_async_status status;
	char *sql;
	int sequence;
	struct mysql_node *next;
} MysqlNode;

MysqlNode *head = NULL;
MYSQL *mysql_conn;
MYSQL_ROW row;
MYSQL_RES *result;
enum net_async_status status;
int sequence = ALL_ROWS_FETCHED;
int last_row_fetched = 0;
char sql[1024] = "SELECT url FROM crawled WHERE frontier = 1 ORDER BY id";
char delete_sql[8192];
char *url = NULL;

void
mysql_start()
{
	if (debug)
		fprintf(stderr, "Calling mysql_init on all nodes.\n");

	head = (MysqlNode *)malloc(sizeof(MysqlNode));
	if (head == NULL)
	{
		fprintf(stderr, "malloc returned NULL, out of memory\n");
		exit(EXIT_FAILURE);
	}
	head->mysql_conn = mysql_init(NULL);
	head->sql = NULL;
	if (head->mysql_conn == NULL)
	{
		fprintf(stderr, "%s\n", mysql_error(head->mysql_conn));
                exit(1);
	}
	head->sequence = READY;

	MysqlNode *current = head;

	for (int i = 0; i < MAX_CONNECTIONS; i++)
	{
		current->next = (MysqlNode *)malloc(sizeof(MysqlNode));
		if (current->next == NULL)
		{
			fprintf(stderr, "malloc returned NULL, out of memory\n");
			exit(EXIT_FAILURE);
		}
		current = current->next;
		current->mysql_conn = mysql_init(NULL);
		if (current->mysql_conn == NULL)
	        {
        	        fprintf(stderr, "%s\n", mysql_error(current->mysql_conn));
        	        exit(1);
       		}
		current->sql = NULL;
		current->sequence = READY;
		current->next = NULL;
	}

	mysql_conn = mysql_init(NULL);
	if (mysql_conn == NULL)
        {
                fprintf(stderr, "%s\n", mysql_error(mysql_conn));
                exit(1);
        }

	if (debug)
		fprintf(stderr, "Connecting all nodes.\n");

	current = head;
	while (current != NULL)
	{
		current->status = mysql_real_connect_nonblocking(current->mysql_conn, "localhost", "crawler", "1q2w3e4r", "crawl", 0, NULL, 0);
		current = current->next;
	}

	status = mysql_real_connect_nonblocking(mysql_conn, "localhost", "crawler", "1q2w3e4r", "crawl", 0, NULL, 0);

	current = head;
	while (current != NULL)
	{
		while (current->status = mysql_real_connect_nonblocking(current->mysql_conn, "localhost", "crawler", "1q2w3e4r", "crawl", 0, NULL, 0) == NET_ASYNC_NOT_READY)
			/* empty loop */
		if (current->status == NET_ASYNC_ERROR)
		{
			fprintf(stderr, "mysql_real_connect_nonblocking: %s\n", mysql_error(current->mysql_conn));
                	exit(EXIT_FAILURE);
		}
		current = current->next;
	}

	while (status = mysql_real_connect_nonblocking(mysql_conn, "localhost", "crawler", "1q2w3e4r", "crawl", 0, NULL, 0) == NET_ASYNC_NOT_READY)
		/*empty loop*/
	if (status == NET_ASYNC_ERROR)
	{
		fprintf(stderr, "mysql_real_connect_nonblocking: %s\n", mysql_error(mysql_conn));
		exit(EXIT_FAILURE);
	}
	
	if (debug)
		fprintf(stderr, "All nodes connected.\n");
}

void
mysql_stop()
{
	MysqlNode *current = head;

	while (current != NULL)
	{
		MysqlNode *next = current->next;
		mysql_close(current->mysql_conn);
		free(current->sql);
		free(current);
		current = next;
	}

	mysql_close(mysql_conn);
}

int
starts_with(const char *str, const char *pre)
{
        size_t lenstr;
        size_t lenpre;

        if (str == NULL || pre == NULL)
                return (-1);

        lenstr = strlen(str);
        lenpre = strlen(pre);

        if (lenstr < lenpre)
                return (-1);

    return (memcmp(pre, str, lenpre));
}

char *
url_sanitize(char *base_url, char *url, int size)
{
        char *newurl;
        int base_url_len = strlen(base_url);

        if (starts_with(url, "http") == 0) {
                newurl = malloc(size+1);
                if (newurl == NULL) {
                        fprintf(stderr, "1 malloc() of %d bytes, failed\n", size);
                        exit(1);
                }

                strncpy(newurl, url, size);
                newurl[size] = '\0';

        } else {
                if (starts_with(url, "//") == 0) {
                        newurl = malloc(size+7);
                        if (newurl == NULL) {
                                fprintf(stderr, "2 malloc() of %d bytes, failed\n", size);
                                exit(1);
                        }

                        strncpy(newurl, "https:", 6);
                        strncpy(newurl+6, url, size);
                        newurl[size+6] = '\0';
                } else {
                        newurl = malloc(base_url_len + size + 2);
                        if (newurl == NULL) {
                                fprintf(stderr, "3 malloc() of %d bytes, failed\n", size);
                                exit(1);
                        }

                        strncpy(newurl, base_url, base_url_len);
                        strncpy(newurl + base_url_len, url, size);
                        newurl[size + base_url_len] = '\0';
                }
        }

        return (newurl);
}

char *
get_host_from_url(char *url)
{
        int ret, start = 0, size = 0, i;
        char *c, *newurl;

        if (url == NULL)
                return (NULL);

        /* XXX: match case insensitive */
        ret = starts_with(url, "https://");
        if (ret != 0) {

                ret = starts_with(url, "http://");
                if (ret == 0)
                        start = 7;

        } else {
                start = 8;
        }

        c = strchr(url+start, '/');
        if (c != NULL)
                size = c - (url + start);
        else
                size = strlen(url+start);

        newurl = malloc(size + 1);
        if (newurl == NULL)
                return (NULL);

        memcpy(newurl, url+start, size);
        newurl[size] = '\0';

        for (i = 0; i < strlen(newurl); i++)
                newurl[i] = tolower(newurl[i]);

        return (newurl);
}

char *
html_title_find(char *html)
{
        char *newurl, *first, *last;
        int size = 0;

        first = strstr(html, "<title>");
        if (first == NULL)
                return (NULL);

        first += strlen("<title>");

        last = strstr(first, "</title>");
        if (last == NULL)
                return (NULL);

        size = last - first;

        newurl = malloc(size+1);
        if (newurl == NULL) {
                fprintf(stderr, "4 malloc() of %d bytes, failed\n", size);
                exit(1);
        }

        strncpy(newurl, first, size);
        newurl[size] = '\0';

        return (newurl);
}

void
html_mailto_find(char *html)
{
        char *spare_first, *first, *last, *email;
        int size = 0;

        first = html;
	last = html;

        while (first && last) {
                char *end;
		spare_first = first;
                first = strstr(first, "href=\"mailto://");
                if (first == NULL)
		{
			first = spare_first;
			first = strstr(first, "href=\"mailto:");
			if (first == NULL)	
                        	continue;
			else
				first += strlen("href=\"mailto:");
		}else{
                	first += strlen("href=\"mailto://");
		}

                last = strchr(first, '\"');
                if (last == NULL)
                        continue;

                end = strchr(first, '?');
                if (end == NULL || end > last){
                        size = last - first;
                } else {
                        size = end - first;
                }

                email = strndup(first, size);

		if (sql_head == NULL)
                {
                        sql_head = (SqlNode *)malloc(sizeof(SqlNode));
			if (sql_head == NULL)
			{
				fprintf(stderr, "malloc returned NULL, out of memory\n");
				exit(EXIT_FAILURE);
			}
                        sql_current = sql_head;
                }
                else
                {
                        sql_current->next = (SqlNode *)malloc(sizeof(SqlNode));
			if (sql_current->next == NULL)
			{
				fprintf(stderr, "malloc returned NULL, out of memory\n");
				exit(EXIT_FAILURE);
			}
                        sql_current = sql_current->next;
                }

		char escaped_email[(strlen(email)*2)+1];
		if (!mysql_real_escape_string(mysql_conn, escaped_email, email, strlen(email)))
		{
		}
		int ret = snprintf(sql_current->sql, sizeof(sql_current->sql), "INSERT IGNORE INTO emails (email) VALUES ('%s')", escaped_email);
		if (ret >= 0 && ret < sizeof(sql_current->sql))
		{
			
		}
		else
		{
			if (debug)
				fprintf(stderr, "%s was too large for buffer\n", escaped_email);
		}
		sql_current->next = NULL;
		free(email);
        }
}

void
html_link_find(char *url, char *html)
{
        char *first, *last, *newurl;
        int size = 0;

        first = html;
	last = html;

        while (first && last) {
		first = strstr(first, "href=\"");
		if (first == NULL)
			continue;

		first += strlen("href=\"");

		last = strchr(first, '\"');
		if (last == NULL)
			continue;

		size = last - first;

		newurl = url_sanitize(url, first, size);

		if (strstr(newurl, "mailto")) {
			free(newurl);
			continue;
		} else {
			if (sql_head == NULL)
			{
				sql_head = (SqlNode *)malloc(sizeof(SqlNode));
				if (sql_head == NULL)
				{
					fprintf(stderr, "malloc returned NULL, out of memory\n");
					exit(EXIT_FAILURE);
				}
				sql_current = sql_head;
			}
                	else
                	{
                	        sql_current->next = (SqlNode *)malloc(sizeof(SqlNode));
				if (sql_current->next == NULL)
				{
					fprintf(stderr, "malloc returned NULL, out of memory\n");
					exit(EXIT_FAILURE);
				}
                        	sql_current = sql_current->next;
                	}

			char escaped_url[(strlen(newurl)*2)+1];
			if (!mysql_real_escape_string(mysql_conn, escaped_url, newurl, strlen(newurl)))
			{}
			int ret = snprintf(sql_current->sql, sizeof(sql_current->sql), "INSERT IGNORE INTO crawled (url) VALUES ('%s')", escaped_url);
			if (ret >= 0 && ret < sizeof(sql_current->sql))
			{
				
			}
			else
			{
				if (debug)
					fprintf(stderr, "%s was too large for buffer\n", escaped_url);
			}
				
			sql_current->next = NULL;
			sql_current->next = (SqlNode *)malloc(sizeof(SqlNode));
			if (sql_current->next == NULL)
			{
				fprintf(stderr, "malloc returned NULL, out of memory\n");
				exit(EXIT_FAILURE);
			}
			sql_current = sql_current->next;

			char *host1, *host2;
			host1 = get_host_from_url(url);
			host2 = get_host_from_url(newurl);

			if (host1 == NULL || host2 == NULL)
			{
				fprintf(stderr, "malloc returned NULL, out of memory\n");
                                exit(EXIT_FAILURE);
			}

			if (strcmp(host1, host2) == 0){
				ret = snprintf(sql_current->sql, sizeof(sql_current->sql), "UPDATE crawled SET links = links + 1 WHERE url = '%s'", escaped_url);
				if (ret >= 0 && ret < sizeof(sql_current->sql))
				{
				
				}
				else
				{
					if (debug)
						fprintf(stderr, "%s was too large for buffer\n", escaped_url);
				}
			}
			else
			{
				ret = snprintf(sql_current->sql, sizeof(sql_current->sql), "UPDATE crawled SET backlinks = backlinks + 1 WHERE url = '%s'", escaped_url);
				if (ret >= 0 && ret < sizeof(sql_current->sql))
				{
				
				}
				else
				{
					if (debug)
						fprintf(stderr, "%s was too large for buffer\n", escaped_url);
				}
			}
			sql_current->next = NULL;

			free(host1);
			free(host2);
			free(newurl);
		}
        }
}

void
parsed_sites_inc(GlobalInfo *g)
{
        g->parsed_sites++;
}

void
html_parse(char *url, char *html)
{
	char *title;
	
	title = html_title_find(html);

	if (title != NULL)
        {
		if (sql_head == NULL)
		{
			sql_head = (SqlNode *)malloc(sizeof(SqlNode));
			if (sql_head == NULL)
			{
				fprintf(stderr, "malloc returned NULL, out of memory\n");
				exit(EXIT_FAILURE);
			}
			sql_current = sql_head;
		}
		else
		{
			sql_current->next = (SqlNode *)malloc(sizeof(SqlNode));
			if (sql_current->next == NULL)
			{
				fprintf(stderr, "malloc returned NULL, out of memory\n");
				exit(EXIT_FAILURE);
			}
			sql_current = sql_current->next;
		}
		char escaped_url[(strlen(url)*2)+1];
		if (!mysql_real_escape_string(mysql_conn, escaped_url, url, strlen(url)))
        	{
        	}
                char escaped_title[(strlen(title)*2)+1];
                if (!mysql_real_escape_string(mysql_conn, escaped_title, title, strlen(title)))
                {
                }
		sprintf(sql_current->sql, "UPDATE crawled SET title = '%s' WHERE url = '%s'", escaped_title, escaped_url);
		sql_current->next = NULL;
	}

	html_link_find(url, html);
	html_mailto_find(html);
	free(title);
}

/* Die if we get a bad CURLMcode somewhere */ 
static void
mcode_or_die(const char *where, CURLMcode code)
{
	if (CURLM_OK != code) {
		const char *s;

		switch (code) {
			mycase(CURLM_BAD_HANDLE); break;
			mycase(CURLM_BAD_EASY_HANDLE); break;
			mycase(CURLM_OUT_OF_MEMORY); break;
			mycase(CURLM_INTERNAL_ERROR); break;
			mycase(CURLM_UNKNOWN_OPTION); break;
			mycase(CURLM_LAST); break;
			default: s = "CURLM_unknown"; break;
			mycase(CURLM_BAD_SOCKET);
			fprintf(MSG_OUT, "ERROR: %s returns %s\n", where, s);
			/* ignore this error */ 
			return;
		}

		fprintf(MSG_OUT, "ERROR: %s returns %s\n", where, s);
		exit(code);
	}
}

void
print_progress(GlobalInfo *g)
{
	printf("\rParsed sites: %d, %d parallel connections, %d still running, %d transfers\t", 
			g->parsed_sites, g->concurrent_connections, g->still_running, g->transfers);
	fflush(stdout);
}

void
transfers_inc(GlobalInfo *g)
{
	g->transfers++;

	print_progress(g);
}

void
transfers_dec(GlobalInfo *g)
{
	g->transfers--;

	print_progress(g);
}

void
concurrent_connections_inc(GlobalInfo *g)
{
	g->concurrent_connections++;

	print_progress(g);
}

void
concurrent_connections_dec(GlobalInfo *g)
{
	g->concurrent_connections--;

	print_progress(g);
}

static void timer_cb(GlobalInfo* g, int revents);
 
/* Update the timer after curl_multi library does it's thing. Curl will
 * inform us through this callback what it wants the new timeout to be,
 * after it does some work. */ 
static int
multi_timer_cb(CURLM *multi, long timeout_ms, GlobalInfo *g)
{
	struct itimerspec its;
 
	//fprintf(MSG_OUT, "multi_timer_cb: Setting timeout to %ld ms\n", timeout_ms);
 
	if (timeout_ms > 0) {
		its.it_interval.tv_sec = 1;
		its.it_interval.tv_nsec = 0;
		its.it_value.tv_sec = timeout_ms / 1000;
		its.it_value.tv_nsec = (timeout_ms % 1000) * 1000 * 1000;
	} else if(timeout_ms == 0) {
		/* libcurl wants us to timeout now, however setting both fields of
		 * new_value.it_value to zero disarms the timer. The closest we can
		 * do is to schedule the timer to fire in 1 ns. */ 
		its.it_interval.tv_sec = 1;
		its.it_interval.tv_nsec = 0;
		its.it_value.tv_sec = 0;
		its.it_value.tv_nsec = 1;
	} else {
		memset(&its, 0, sizeof(struct itimerspec));
	}
 
	timerfd_settime(g->tfd, /*flags=*/ 0, &its, NULL);

	return (0);
}
 
/* Check for completed transfers, and remove their easy handles */ 
static void
check_multi_info(GlobalInfo *g)
{
	char *eff_url;
	CURLMsg *msg;
	int msgs_left;
	ConnInfo *conn;
	CURL *easy;
	char *ct;
	double time;
	double dl;
	long header_size;
	long response_code;
	//CURLcode res;
 
	while ((msg = curl_multi_info_read(g->multi, &msgs_left))) {
		if (msg->msg == CURLMSG_DONE) {
			easy = msg->easy_handle;
			//res = msg->data.result;
			curl_easy_getinfo(easy, CURLINFO_PRIVATE, &conn);
			curl_easy_getinfo(easy, CURLINFO_EFFECTIVE_URL, &eff_url);
			curl_easy_getinfo(easy, CURLINFO_CONTENT_TYPE, &ct);
			curl_easy_getinfo(easy, CURLINFO_TOTAL_TIME, &time);
			curl_easy_getinfo(easy, CURLINFO_SIZE_DOWNLOAD, &dl);
			curl_easy_getinfo(easy, CURLINFO_RESPONSE_CODE, &response_code);
			curl_easy_getinfo(easy, CURLINFO_HEADER_SIZE, &header_size);

			if (response_code == 200 && dl == 0.0 && (starts_with(ct, "text/html") || starts_with(ct, "text/plain")))
			{
				/* This should be a response to our HEAD request */
				//printf("200 %s header size: %ld download size: %f", eff_url, header_size, dl);
				new_body_conn(eff_url, g);

			} else if (response_code == 200 && dl > 0.0 && (starts_with(ct, "text/html") || starts_with(ct, "text/plain"))){
				/* This should be a response to our GET request */
				//printf("%ld %s download size: %f content type: %s\n", response_code, eff_url, dl, ct);
				html_parse(eff_url, conn->data);
				parsed_sites_inc(g);
			}
			//fprintf(MSG_OUT, "DONE: %s => (%d) %s\n", eff_url, res, conn->error);

			curl_multi_remove_handle(g->multi, easy);
			free(conn->url);
			free(conn->data);
			curl_easy_cleanup(easy);
			transfers_dec(g);
			free(conn);
		}
	}
}
 
/* Called by libevent when we get action on a multi socket filedescriptor*/ 
static void
event_cb(GlobalInfo *g, int fd, int revents)
{
	CURLMcode rc;
	struct itimerspec its;
 
	int action = ((revents & EPOLLIN) ? CURL_CSELECT_IN : 0) |
				 ((revents & EPOLLOUT) ? CURL_CSELECT_OUT : 0);
 
	rc = curl_multi_socket_action(g->multi, fd, action, &g->still_running);
	mcode_or_die("event_cb: curl_multi_socket_action", rc);
 
	check_multi_info(g);

	if (g->still_running <= 0) {
		//fprintf(MSG_OUT, "last transfer done, kill timeout\n");
		memset(&its, 0, sizeof(struct itimerspec));
		timerfd_settime(g->tfd, 0, &its, NULL);
	}
}
 
/* Called by main loop when our timeout expires */ 
static void
timer_cb(GlobalInfo* g, int revents)
{
	CURLMcode rc;
	uint64_t count = 0;
	ssize_t err = 0;
 
	err = read(g->tfd, &count, sizeof(uint64_t));
	if (err == -1) {
		/* Note that we may call the timer callback even if the timerfd isn't
		 * readable. It's possible that there are multiple events stored in the
		 * epoll buffer (i.e. the timer may have fired multiple times). The
		 * event count is cleared after the first call so future events in the
		 * epoll buffer will fail to read from the timer. */ 
		if (errno == EAGAIN) {
			//fprintf(MSG_OUT, "EAGAIN on tfd %d\n", g->tfd);
			return;
		}
	}

	if (err != sizeof(uint64_t)) {
		fprintf(stderr, "read(tfd) == %ld", err);
		perror("read(tfd)");
	}
 
	rc = curl_multi_socket_action(g->multi, CURL_SOCKET_TIMEOUT, 0, &g->still_running);
	mcode_or_die("timer_cb: curl_multi_socket_action", rc);
	check_multi_info(g);
}

/* Assign information to a SockInfo structure */ 
static void
setsock(SockInfo *f, curl_socket_t s, CURL *e, int act, GlobalInfo *g)
{
	struct epoll_event ev;
	int kind = ((act & CURL_POLL_IN) ? EPOLLIN : 0) |
			   ((act & CURL_POLL_OUT) ? EPOLLOUT : 0);
 
	if (f->sockfd) {
		concurrent_connections_dec(g);
		if (epoll_ctl(g->epfd, EPOLL_CTL_DEL, f->sockfd, NULL))
			fprintf(stderr, "EPOLL_CTL_DEL failed for fd: %d : %s\n",
			  f->sockfd, strerror(errno));
	}
 
	f->sockfd = s;
	f->action = act;
	f->easy = e;
 
	memset(&ev, 0, sizeof(ev));
	ev.events = kind;
	ev.data.fd = s;

	concurrent_connections_inc(g);
	if (epoll_ctl(g->epfd, EPOLL_CTL_ADD, s, &ev)) {
		fprintf(stderr, "EPOLL_CTL_ADD failed for fd: %d : %s\n",
		  s, strerror(errno));
	}
}
 
/* Initialize a new SockInfo structure */ 
static void
addsock(curl_socket_t s, CURL *easy, int action, GlobalInfo *g)
{
	SockInfo *fdp = (SockInfo *)calloc(sizeof(SockInfo), 1);
 
	fdp->global = g;
	setsock(fdp, s, easy, action, g);
	curl_multi_assign(g->multi, s, fdp);
}
 
static size_t
write_cb(void *contents, size_t size, size_t nmemb, void *p)
{
	ConnInfo *conn = (ConnInfo *)p;
	size_t realsize = size * nmemb;

	conn->data = realloc(conn->data, conn->size + realsize + 1);
	if (conn->data == NULL) {
		/* out of memory! */ 
		printf("not enough memory (realloc returned NULL)\n");
		return 0;
	}
 
	memcpy(&(conn->data[conn->size]), contents, realsize);
	conn->size += realsize;
	conn->data[conn->size] = 0;
 
	return realsize;
}

/* Create a new easy handle, and add it to the global curl_multi */ 
int
new_head_conn(char *url, GlobalInfo *g)
{
	ConnInfo *conn;
	CURLMcode rc;

	conn = (ConnInfo*)calloc(1, sizeof(ConnInfo));
	conn->error[0]='\0';
	conn->global = g;
 
	conn->easy = curl_easy_init();
	if (!conn->easy) {
		fprintf(MSG_OUT, "curl_easy_init() failed, exiting!\n");
		exit(2);
	}
	transfers_inc(g);

	conn->global = g;
	conn->url = strdup(url);
	free(url);
	curl_easy_setopt(conn->easy, CURLOPT_URL, conn->url);
	curl_easy_setopt(conn->easy, CURLOPT_WRITEFUNCTION, write_cb);
	curl_easy_setopt(conn->easy, CURLOPT_WRITEDATA, conn);
	curl_easy_setopt(conn->easy, CURLOPT_FOLLOWLOCATION, 1L);
	curl_easy_setopt(conn->easy, CURLOPT_ERRORBUFFER, conn->error);
	curl_easy_setopt(conn->easy, CURLOPT_PRIVATE, conn);
	curl_easy_setopt(conn->easy, CURLOPT_NOPROGRESS, 1L);
	curl_easy_setopt(conn->easy, CURLOPT_PROGRESSDATA, conn);
	curl_easy_setopt(conn->easy, CURLOPT_FOLLOWLOCATION, 1L);
	curl_easy_setopt(conn->easy, CURLOPT_LOW_SPEED_TIME, 3L);
	curl_easy_setopt(conn->easy, CURLOPT_LOW_SPEED_LIMIT, 100L);
	curl_easy_setopt(conn->easy, CURLOPT_CONNECTTIMEOUT, 10L);
	curl_easy_setopt(conn->easy, CURLOPT_CLOSESOCKETDATA, g);
	curl_easy_setopt(conn->easy, CURLOPT_NOBODY, 1L);

	rc = curl_multi_add_handle(g->multi, conn->easy);
	mcode_or_die("new_conn: curl_multi_add_handle", rc);
 
	/* note that the add_handle() will set a time-out to trigger very soon so
     that the necessary socket_action() call will be called by this app */ 

	return (0);
}
 
/* Create a new easy handle, and add it to the global curl_multi */
int
new_body_conn(char *url, GlobalInfo *g)
{
        ConnInfo *conn;
        CURLMcode rc;

        conn = (ConnInfo*)calloc(1, sizeof(ConnInfo));
        conn->error[0]='\0';
        conn->global = g;

        conn->easy = curl_easy_init();
        if (!conn->easy) {
                fprintf(MSG_OUT, "curl_easy_init() failed, exiting!\n");
                exit(2);
        }
        transfers_inc(g);

        conn->global = g;
        conn->url = strdup(url);
	free(url);
        curl_easy_setopt(conn->easy, CURLOPT_URL, conn->url);
        curl_easy_setopt(conn->easy, CURLOPT_WRITEFUNCTION, write_cb);
        curl_easy_setopt(conn->easy, CURLOPT_WRITEDATA, conn);
        curl_easy_setopt(conn->easy, CURLOPT_FOLLOWLOCATION, 1L);
        curl_easy_setopt(conn->easy, CURLOPT_ERRORBUFFER, conn->error);
        curl_easy_setopt(conn->easy, CURLOPT_PRIVATE, conn);
        curl_easy_setopt(conn->easy, CURLOPT_NOPROGRESS, 1L);
        curl_easy_setopt(conn->easy, CURLOPT_PROGRESSDATA, conn);
        curl_easy_setopt(conn->easy, CURLOPT_FOLLOWLOCATION, 1L);
        curl_easy_setopt(conn->easy, CURLOPT_LOW_SPEED_TIME, 3L);
        curl_easy_setopt(conn->easy, CURLOPT_LOW_SPEED_LIMIT, 100L);
        curl_easy_setopt(conn->easy, CURLOPT_CONNECTTIMEOUT, 10L);
        curl_easy_setopt(conn->easy, CURLOPT_CLOSESOCKETDATA, g);

        rc = curl_multi_add_handle(g->multi, conn->easy);
        mcode_or_die("new_conn: curl_multi_add_handle", rc);

        /* note that the add_handle() will set a time-out to trigger very soon so
     that the necessary socket_action() call will be called by this app */

        return (0);
}

/* Clean up the SockInfo structure */ 
static void
remsock(SockInfo *f, GlobalInfo* g)
{
	if (f) {
		if (f->sockfd) {
			concurrent_connections_dec(g);
			if (epoll_ctl(g->epfd, EPOLL_CTL_DEL, f->sockfd, NULL))
				fprintf(stderr, "EPOLL_CTL_DEL failed for fd: %d : %s\n",
				  f->sockfd, strerror(errno));
		}

		free(f);
	}
}
 
/* CURLMOPT_SOCKETFUNCTION */ 
static int
sock_cb(CURL *e, curl_socket_t s, int what, void *cbp, void *sockp)
{
	GlobalInfo *g = (GlobalInfo*) cbp;
	SockInfo *fdp = (SockInfo*) sockp;

	if (what == CURL_POLL_REMOVE) {
		remsock(fdp, g);
	} else {
		if (g->concurrent_connections < MAX_CONNECTIONS){
			if (!fdp) {
				addsock(s, e, what, g);
			} else {
				setsock(fdp, s, e, what, g);
			}
		}
	}

	return (0);
}

/* CURLMOPT_SOCKETFUNCTION */
static int
end_sock_cb(CURL *e, curl_socket_t s, int what, void *cbp, void *sockp)
{
        GlobalInfo *g = (GlobalInfo*) cbp;
        SockInfo *fdp = (SockInfo*) sockp;

        if (what == CURL_POLL_REMOVE) {
                remsock(fdp, g);
        }

        return (0);
}


int should_exit = 0;
 
void
signal_handler(int signo)
{
	should_exit = 1;
}

void *
crawler_init()
{
	GlobalInfo g;
	struct itimerspec its;
	struct epoll_event ev;
	struct epoll_event events[10000];


	memset(&g, 0, sizeof(GlobalInfo));

	g.transfers = 0;
	g.parsed_sites = 0;

	g.epfd = epoll_create1(EPOLL_CLOEXEC);
	if (g.epfd == -1) {
		perror("epoll_create1 failed\n");
		exit(1);
	}
 
	g.tfd = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC);
	if (g.tfd == -1) {
		perror("timerfd_create failed\n");
		exit(1);
	}
 
	memset(&its, 0, sizeof(struct itimerspec));
	its.it_interval.tv_sec = 1;
	its.it_value.tv_sec = 1;
	timerfd_settime(g.tfd, 0, &its, NULL);
 
	memset(&ev, 0, sizeof(ev));
	ev.events = EPOLLIN;
	ev.data.fd = g.tfd;
	epoll_ctl(g.epfd, EPOLL_CTL_ADD, g.tfd, &ev);
 
	curl_global_init(CURL_GLOBAL_DEFAULT);
	g.multi = curl_multi_init();
 
	/* setup the generic multi interface options we want */ 
	curl_multi_setopt(g.multi, CURLMOPT_SOCKETFUNCTION, sock_cb);
	curl_multi_setopt(g.multi, CURLMOPT_SOCKETDATA, &g);
	curl_multi_setopt(g.multi, CURLMOPT_TIMERFUNCTION, multi_timer_cb);
	curl_multi_setopt(g.multi, CURLMOPT_TIMERDATA, &g);

	/* we don't call any curl_multi_socket*() function yet as we have no handles added! */ 

	if (debug)
		printf("Starting crawler...\n");

	while (!should_exit) {
		int idx;
		int err = epoll_wait(g.epfd, events, sizeof(events)/sizeof(struct epoll_event), 10000);

		/* Pop url from frontier in crawled table and crawl it */
		if (sequence == ALL_ROWS_FETCHED)
		{
			if (debug)
				printf("Doing select\n");
			status = mysql_real_query_nonblocking(mysql_conn, sql, (unsigned long)strlen(sql));

			if (status == NET_ASYNC_COMPLETE)
			{
				sequence = SELECT_DONE;
			} 
			else if (status == NET_ASYNC_ERROR)
			{
				fprintf(stderr, "mysql_real_query_nonblocking: %s",mysql_error(mysql_conn));
				exit(EXIT_FAILURE);
			}
		} 
		else if (sequence == SELECT_DONE) 
		{	
			if (debug)
				printf("Select done. Storing result\n");
			status = mysql_store_result_nonblocking(mysql_conn, &result);

			if (status == NET_ASYNC_COMPLETE)
                        {
                                sequence = FETCHED_RESULT;
                        }
                        else if (status == NET_ASYNC_ERROR)
                        {
                                fprintf(stderr, "mysql_store_result_nonblocking: %s",mysql_error(mysql_conn));
                                exit(EXIT_FAILURE);
                        }
		}
		else if (sequence == FETCHED_RESULT)
		{
			if (debug)
				printf("Result stored. Fetching row\n");
			status = mysql_fetch_row_nonblocking(result, &row);

			if (status == NET_ASYNC_COMPLETE)
                        {
                                sequence = FETCHED_ROW;
				if (row == NULL)
				{
					if (debug)
						printf("No more rows.\n");
					last_row_fetched = 1;
				}
				else
				{
					url = strdup(row[0]);
					if (debug)
						printf("url: %s\n", url);
					new_head_conn(url, &g);
					//free(url);
					//url = NULL;
				}
                        }
                        else if (status == NET_ASYNC_ERROR)
                        {
                                fprintf(stderr, "mysql_store_result_nonblocking: %s",mysql_error(mysql_conn));
                                exit(EXIT_FAILURE);
                        }
		}
		else if (sequence == FETCHED_ROW)
		{
			if (url != NULL)
			{
				if (debug)
					printf("Fetched row. Deleting url from frontier\n");
				char escaped_url[(strlen(url)*2)+1];
				if (!mysql_real_escape_string(mysql_conn, escaped_url, url, strlen(url)))
				{}
				sprintf( delete_sql, "UPDATE crawled SET frontier = 0 WHERE url = '%s'", escaped_url);
				status = mysql_real_query_nonblocking(mysql_conn, delete_sql, (unsigned long)strlen(delete_sql));
				if (status == NET_ASYNC_COMPLETE)
                       		{
					sequence = DELETE_DONE;
				}
				else if (status == NET_ASYNC_ERROR)
                        	{
                        	        fprintf(stderr, "mysql_real_query_nonblocking: %s sql: %s\n",mysql_error(mysql_conn), sql);
                        	        exit(EXIT_FAILURE);
                        	}
			}
		}
		else if (sequence == DELETE_DONE)
		{
			if (last_row_fetched == 1)
			{
				if (debug)
					fprintf(stderr, "Last row fetched\n");
				status = mysql_free_result_nonblocking(result);
				if (status == NET_ASYNC_COMPLETE)
				{
					last_row_fetched = 0;
					sequence = ALL_ROWS_FETCHED;
				}
				else if (status == NET_ASYNC_ERROR)
				{
					fprintf(stderr, "mysql_free_result_nonblocking: %s\n", mysql_error(mysql_conn));
					exit(EXIT_FAILURE);
				}
			}
			else
			{
				if (debug)
					fprintf(stderr, "Fetching next row\n");
				sequence = FETCHED_RESULT;
			}

		}


		MysqlNode *current = head;
		while (current != NULL)
		{
			if (current->sequence == READY)
			{
				if(sql_head != NULL)
				{
					SqlNode *sql_tmp = sql_head->next;
					free(current->sql);
					current->sql = strdup(sql_head->sql);
					free(sql_head);
					sql_head = sql_tmp;
					current->status = mysql_real_query_nonblocking(current->mysql_conn, current->sql, (unsigned long)strlen(current->sql));
				}
			}
			else if (current->sequence == BUSY)
			{
				current->status = mysql_real_query_nonblocking(current->mysql_conn, current->sql, (unsigned long)strlen(current->sql));
				if (current->status == NET_ASYNC_COMPLETE)
				{
					current->sequence = READY;
				}
				else if (current->status == NET_ASYNC_ERROR)
				{
					fprintf(stderr, "mysql_real_query_nonblocking: %s\n", mysql_error(current->mysql_conn));
                                        exit(EXIT_FAILURE);
				}
			}
			current = current->next;
		}

		if (err == -1) {
			if (errno == EINTR) {
				fprintf(MSG_OUT, "note: wait interrupted\n");
				continue;
			} else {
				perror("epoll_wait");
				exit(1);
			}
		}
 
		for (idx = 0; idx < err; ++idx) {
			if (events[idx].data.fd == g.tfd) {
				timer_cb(&g, events[idx].events);
			} else {
				event_cb(&g, events[idx].data.fd, events[idx].events);
			}
		}
	}
 
	fprintf(MSG_OUT, "Exiting normally.\n");
	fflush(MSG_OUT);
 
	curl_multi_setopt(g.multi, CURLMOPT_SOCKETFUNCTION, end_sock_cb);
	while (g.concurrent_connections > 0 || g.transfers > 0)
	{
		int idx;
                int err = epoll_wait(g.epfd, events, sizeof(events)/sizeof(struct epoll_event), 10000);

		MysqlNode *current = head;
		while (current != NULL)
		{
			if (current->sequence == READY)
			{
				if(sql_head != NULL)
				{
					SqlNode *sql_tmp = sql_head->next;
					free(current->sql);
					current->sql = strdup(sql_head->sql);
					free(sql_head);
					sql_head = sql_tmp;
					current->status = mysql_real_query_nonblocking(current->mysql_conn, current->sql, (unsigned long)strlen(current->sql));
				}
			}
			else if (current->sequence == BUSY)
			{
				while (current->status = mysql_real_query_nonblocking(current->mysql_conn, current->sql, (unsigned long)strlen(current->sql)) == NET_ASYNC_NOT_READY);
					/* empty loop */
				if (current->status == NET_ASYNC_COMPLETE)
				{
					current->sequence = READY;
				}
				else if (current->status == NET_ASYNC_ERROR)
				{
					fprintf(stderr, "mysql_real_query_nonblocking: %s\n", mysql_error(current->mysql_conn));
                                        exit(EXIT_FAILURE);
				}
			}
			current = current->next;
		}

                if (err == -1) {
                        if (errno == EINTR) {
                                fprintf(MSG_OUT, "note: wait interrupted\n");
                                continue;
                        } else {
                                perror("epoll_wait");
                                exit(1);
                        }
                }

                for (idx = 0; idx < err; ++idx) {
                        if (events[idx].data.fd == g.tfd) {
                                timer_cb(&g, events[idx].events);
                        } else {
                                event_cb(&g, events[idx].data.fd, events[idx].events);
                        }
                }

	}

	fprintf(MSG_OUT, "Finished all in progress downloads.\n");
	fflush(MSG_OUT);

	curl_multi_cleanup(g.multi);
	curl_global_cleanup();

	return (NULL);
}

int
main(int argc, char **argv)
{
	int cleanup = 0, opt, ret;

	should_exit = 0;
	signal(SIGINT, signal_handler);
	signal(SIGKILL, signal_handler);
	sigaction(SIGPIPE, &(struct sigaction){SIG_IGN}, NULL);

	while ((opt = getopt(argc, argv, "d")) != -1)
	{
		switch (opt) 
		{
			case 'd':
				debug = 1;
				break;
			default:
				fprintf(stderr, "Usage: %s [-d]\n", argv[0]);
				exit(EXIT_FAILURE);
		}
	}

	mysql_start();
	crawler_init();
	mysql_stop();


	printf("Exiting.\n");

	return (0);
}
