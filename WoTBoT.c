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
#include <wget.h>

#define MSG_OUT stdout
#define DEFAULT_QUEUE_LENGTH 10000
#define mycase(code) \
        case code: s = __STRING(code)

#define MAX_CONNECTIONS 1024
#define MAX_SQL_CONNECTIONS 1024
#define ALL_ROWS_FETCHED 0
#define SELECT_DONE 1
#define FETCHED_RESULT 2
#define FETCHED_ROW 3
#define DELETE_DONE 4

#define READY 0
#define BUSY 1

#define INITIATE 0
#define STORE 1
#define FREE 2
#define DONE 3

int debug = 0;
int sql_queue = 0;

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
	char *sql;
	struct sql_node *next;
} SqlNode;

SqlNode *sql_head = NULL;
SqlNode *sql_current;

typedef struct mysql_node
{
	MYSQL *mysql_conn;
	MYSQL_RES *result;
	enum net_async_status status;
	char *sql;
	int sequence;
	int sequence02;
	struct mysql_node *next;
} MysqlNode;

MysqlNode *head = NULL;
MYSQL *mysql_conn;
MYSQL *mysql_delete_conn;
MYSQL_ROW row;
MYSQL_RES *result;
enum net_async_status status;
enum net_async_status delete_status;
int sequence = ALL_ROWS_FETCHED;
int last_row_fetched = 0;
char sql[1024] = "SELECT url FROM crawled WHERE frontier = 1 ORDER BY id";
char *delete_sql = NULL;
char *global_url = NULL;
int memory = 0;

void
mysql_start()
{
	if (debug)
		fprintf(stderr, "Calling mysql_init on all nodes.\n");

	head = (MysqlNode *)malloc(sizeof(MysqlNode));
	memory += sizeof(MysqlNode);
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

	for (int i = 0; i < MAX_SQL_CONNECTIONS; i++)
	{
		current->next = (MysqlNode *)malloc(sizeof(MysqlNode));
		memory += sizeof(MysqlNode);
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

	mysql_delete_conn = mysql_init(NULL);
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
		current->status = mysql_real_connect_nonblocking(current->mysql_conn, "localhost", "crawler", "1q2w3e4r", "crawl", 0, NULL, CLIENT_MULTI_STATEMENTS);
		current = current->next;
	}

	status = mysql_real_connect_nonblocking(mysql_conn, "localhost", "crawler", "1q2w3e4r", "crawl", 0, NULL, 0);
	delete_status = mysql_real_connect_nonblocking(mysql_delete_conn, "localhost", "crawler", "1q2w3e4r", "crawl", 0, NULL, 0);

	current = head;
	while (current != NULL)
	{
		while (current->status = mysql_real_connect_nonblocking(current->mysql_conn, "localhost", "crawler", "1q2w3e4r", "crawl", 0, NULL, CLIENT_MULTI_STATEMENTS) == NET_ASYNC_NOT_READY)
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
	
	while (delete_status = mysql_real_connect_nonblocking(mysql_delete_conn, "localhost", "crawler", "1q2w3e4r", "crawl", 0, NULL, 0) == NET_ASYNC_NOT_READY)
		/*empty loop*/
	if (delete_status == NET_ASYNC_ERROR)
	{
		fprintf(stderr, "mysql_real_connect_nonblocking: %s\n", mysql_error(mysql_delete_conn));
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
		if (current->sql != NULL)
			memory -= strlen(current->sql);
		memory -= sizeof(MysqlNode);
		free(current->sql);
		free(current);
		current = next;
	}

	mysql_close(mysql_delete_conn);
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

const char * 
parseURI (char *base_url, char *url)
{
	wget_iri *base = wget_iri_parse(base_url, NULL);
	wget_buffer *buf = wget_buffer_alloc(8192);
	const char *uri = wget_iri_relative_to_abs(base, url, strlen(url), buf);
	memory += strlen(uri);
	buf->data = NULL;
	wget_buffer_free(&buf);
	wget_iri_free(&base);
	return uri;
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
	memory += strlen(newurl);

        for (i = 0; i < strlen(newurl); i++)
                newurl[i] = tolower(newurl[i]);

        return (newurl);
}

void
sql_queue_inc()
{
	sql_queue++;
}

void
sql_queue_dec()
{
	sql_queue--;
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
	memory += strlen(newurl);

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
		memory += strlen(email);

		if (sql_head == NULL)
                {
                        sql_head = (SqlNode *)malloc(sizeof(SqlNode));
			memory += sizeof(SqlNode);
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
			memory += sizeof(SqlNode);
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
		int size = strlen(escaped_email) + strlen("INSERT IGNORE INTO emails (email) VALUES ('')") + 1;
		sql_current->sql = (char *) malloc (size);
		memory += size;
		int ret = snprintf(sql_current->sql, size, "INSERT IGNORE INTO emails (email) VALUES ('%s')", escaped_email);
		if (ret >= 0 && ret <= size)
		{
			
		}
		else
		{
			if (debug)
				fprintf(stderr, "%s was too large for buffer\n", escaped_email);
		}
		sql_queue_inc();
		sql_current->next = NULL;
		memory -= strlen(email);
		free(email);
        }
}

void
html_link_find(char *base_url, char *html)
{
        char *first, *last;
       	const char *newurl;
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

		//newurl = url_sanitize(url, first, size);
		char *url;
                url = malloc(size+1);
                strncpy(url, first, size);
                url[size] = '\0';
		memory += strlen(url);
		newurl = parseURI(base_url, url);
		if (debug)
		{
			fprintf(stderr, "base_url: %s\n", base_url);
			fprintf(stderr, "url: %s\n", url);
			fprintf(stderr, "newurl: %s\n", newurl);
		}
		memory -= strlen(url);
		free(url);
		if (newurl == NULL)
			continue;

		if (strstr(newurl, "mailto")) {
			memory -= strlen(newurl);
			free((char *)newurl);
			continue;
		} else {
			if (sql_head == NULL)
			{
				sql_head = (SqlNode *)malloc(sizeof(SqlNode));
				memory += sizeof(SqlNode);
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
				memory += sizeof(SqlNode);
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
			int size = strlen(escaped_url) + strlen("INSERT IGNORE INTO crawled (url) VALUES ('')") + 1;
			sql_current->sql = (char *) malloc (size);
			memory += size;
			int ret = snprintf(sql_current->sql, size, "INSERT IGNORE INTO crawled (url) VALUES ('%s')", escaped_url);
			if (ret >= 0 && ret <= size)
			{
				
			}
			else
			{
				if (debug)
					fprintf(stderr, "%s was too large for buffer\n", escaped_url);
			}
				
			sql_queue_inc();
			sql_current->next = NULL;
			sql_current->next = (SqlNode *)malloc(sizeof(SqlNode));
			memory += sizeof(SqlNode);
			if (sql_current->next == NULL)
			{
				fprintf(stderr, "malloc returned NULL, out of memory\n");
				exit(EXIT_FAILURE);
			}
			sql_current = sql_current->next;

			char *host1, *host2;
			host1 = get_host_from_url(base_url);
			host2 = get_host_from_url((char *)newurl);
			memory += strlen(host1);
			memory += strlen(host2);

			if (host1 == NULL || host2 == NULL)
			{
				fprintf(stderr, "malloc returned NULL, out of memory\n");
                                exit(EXIT_FAILURE);
			}

			if (strcmp(host1, host2) == 0){
				size = strlen(escaped_url) + strlen("UPDATE crawled SET links = links + 1 WHERE url = ''") + 1;
				sql_current->sql = (char *) malloc (size);
				memory += size;
				ret = snprintf(sql_current->sql, size, "UPDATE crawled SET links = links + 1 WHERE url = '%s'", escaped_url);
				if (ret >= 0 && ret <= size)
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
				size = strlen(escaped_url) + strlen("UPDATE crawled SET backlinks = backlinks + 1 WHERE url = ''") + 1;
				sql_current->sql = (char *) malloc (size);
				memory += size;
				ret = snprintf(sql_current->sql, size, "UPDATE crawled SET backlinks = backlinks + 1 WHERE url = '%s'", escaped_url);
				if (ret >= 0 && ret <= size)
				{
				
				}
				else
				{
					if (debug)
						fprintf(stderr, "%s was too large for buffer\n", escaped_url);
				}
			}
			sql_queue_inc();
			sql_current->next = NULL;

			memory -= strlen(host1);
			memory -= strlen(host2);
			memory -= strlen(newurl);
			free(host1);
			free(host2);
			free((char *)newurl);
		}
        }
}

void
parsed_sites_inc(GlobalInfo *g)
{
        g->parsed_sites++;
}

// These are looked after <script ..and before </script keywords
char AD_TAGS[][100] = {
	"googleads",
	"google_ad_client",
	"adsbygoogle",
	//"googlesyndication",
	//"moatads.com", //MOAT ADS
	"bat.bing.com/action", // THIS IS MODIFIED. Check the bat.js comment immediately below this array
	// DON'T REMOVE this one.
	"pagead2.googlesyndication.com/pagead/js/adsbygoogle.js",
	"contextual.media.net/dmedianet.js"
};
//"bat.bing.com/bat.js" // DONT add this. // bat.bing.com is for advertisement, whereas bat.bing.com/bat.js contains UET
	// UET : Universal Event Tracking allows microsoft to track user events and manipulate it in order to show
	// interest based ads maybe on other sites. You may visit bat.bing.com/bat/js and see there is not any ad related thing. // Probably to show up in bing search engine. They don't contain script, instead bing webpage is
// processed on server and then sent. It does not has ad scripts. But can show ads because they came from server.
// I think the server looks for UET in many sites.

// These are looked after <img or <p or <div ... (before tag closing) ... onlickOrSrcOrSomething="https:...googleads.g.doubleclick.net"
char AD_RENDERED[][100] = {
	"g.doubleclick.net",
	"google_ads_iframe_",
	"google_ads",
	"yap_ad" // Yahoo ad, needs more sources

};

/* Cross origins are the techniques that allow the use of externel sources (CORS) within img, script etc */
char CROSS_ORIGINS[][100] = {
	"node.setAttribute('crossorigin', 'anonymous')",
	"node.setAttribute('crossorigin','anonymous')",

};
//char tagSeparatorStart = '<';
#define EndOfString '\0'
typedef int BOOL;
#define TRUE 1
#define FALSE 0  // CHANGE THIS FALSE TO OTHER NAME BEFORE USING (IF DEFINED A FALSE typedef as #define FALSE 0)


int is_alphabet(char ch)
{
	// I dont think I should use char c = 32 and then bitwise it. There are symbols in html
	char* abcString = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
	for (int i = 0; i < 26; i++)
	{
		if (abcString[i] == ch)
			return 1;
	}
	return 0;
}

// REMOVE is_alphabet if you are sure that all html files will be lowercase
int compareNumberOfChar(char* str1, char* str2, int numberfromstr1)
{
	int j = 0;
	for (int i = 0; i < numberfromstr1; i++)
	{
		if (str1[i] == EndOfString && j > 0)
			break;
		if (is_alphabet(str1[i]) && is_alphabet(str2[i]))
		{
			if ((str1[i] | 32) != (str2[i] | 32))
				return 0;
			j++; // It didn't work as j++ in j > 0 statement
			continue;
		}
		if (str1[i] != str2[i])
		{
			return 0;
		}
		j++;
	}
	return 1;
}

BOOL WithinScript = FALSE;
BOOL WithinTag = FALSE;
//char STRDOUBTED[100];
int has_ads(char* fileContents)
{
	char* scriptStart = "<script"; // don't make it <script>
	char* scriptEnd = "</script";



	char* reader = fileContents;
	BOOL noscriptFound = TRUE;
	BOOL foundOne = FALSE;
	while ((reader[0]) != EndOfString)
	{
		if (compareNumberOfChar(reader, "<", 1)/* && !compareNumberOfChar("<noscript>", reader, 10)*/)
			WithinTag = TRUE;

		if (WithinTag == TRUE)
		{
			if (compareNumberOfChar("noscript", reader, 10))
			{
				/*WithinTag = FALSE;*/
				noscriptFound = TRUE;
			}
			if (compareNumberOfChar("/noscript", reader, 10))
			{
				/*WithinTag = FALSE;*/
				noscriptFound = FALSE;
			}
			if (!noscriptFound)
			for (int i = 0; i < (sizeof(AD_RENDERED) / 100); i++)
			{
				char* gh = AD_RENDERED[i];
				if (compareNumberOfChar(gh, reader, sizeof(AD_RENDERED[i])))
				{
					/*return 1;*/
					foundOne = TRUE;
				}
			}
		}

		/* ONLY CHECK CrossOrgin inside the script tag. Not here*/
		if (compareNumberOfChar(reader, ">", 1))
		{
			WithinTag = FALSE;
		}

		if (compareNumberOfChar(reader, scriptStart, 7))
			WithinScript = TRUE;


		if (WithinScript == TRUE)
		{
			//printf("Got a script tag\n");
			//WithinScript = UNKNOWN;
			// Do search work
			for (int i = 0; i < (sizeof(AD_TAGS) / 100); i++)
			{
				char* gh = AD_TAGS[i];
				if (compareNumberOfChar(gh, reader, sizeof(AD_TAGS[i])))
				{
					foundOne = TRUE;
				}

			}

			for (int i = 0; i < (sizeof(CROSS_ORIGINS) / 100); i++)
			{
				char* gh = CROSS_ORIGINS[i];
				if (compareNumberOfChar(gh, reader, sizeof(CROSS_ORIGINS[i])))
				{
					foundOne = FALSE;
				}
			}


		}
		if (compareNumberOfChar(reader, scriptEnd, 8))
		{
			WithinScript = FALSE;
		}

		++reader;
	}
	if (foundOne)
		return 1;
	return 0; // No Ad upto end of page
	//printf("Got end of string\n");
}

void
html_parse(char *url, char *html)
{
	char *title;
	
	title = html_title_find(html);

	if (sql_head == NULL)
	{
		sql_head = (SqlNode *)malloc(sizeof(SqlNode));
		memory += sizeof(SqlNode);
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
		memory += sizeof(SqlNode);
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
	if (title != NULL)
        {
                char escaped_title[(strlen(title)*2)+1];
                if (!mysql_real_escape_string(mysql_conn, escaped_title, title, strlen(title)))
                {
                }

		if (has_ads(html))
		{
			int size = strlen(escaped_url) + strlen("INSERT IGNORE INTO crawled (url) VALUES ('');") + 
				strlen(escaped_url) + strlen("UPDATE crawled SET frontier = 0 WHERE url = '';") +
				strlen(escaped_url) + strlen(escaped_title) + strlen("UPDATE crawled SET title = '' WHERE url = '';") +
				strlen(escaped_url) + strlen("UPDATE crawled SET ads = 1 WHERE url = ''") + 1;
			sql_current->sql = (char *) malloc (size);
			memory += size;
			int ret = snprintf(sql_current->sql, size, "INSERT IGNORE INTO crawled (url) VALUES ('%s');UPDATE crawled SET frontier = 0 WHERE url = '%s';UPDATE crawled SET title = '%s' WHERE url = '%s';UPDATE crawled SET ads = 1 WHERE url = '%s'", escaped_url, escaped_url, escaped_title, escaped_url, escaped_url);
			if (ret >= 0 && ret <= size)
			{}
			else
			{
				if (debug)
					fprintf(stderr, "%s too big for buffer\n", escaped_url);
			}	
			sql_queue_inc();
			sql_current->next = NULL;
			if (debug)
				fprintf(stderr, "Inserting %s, Updating title %s, Setting ads = 1\n", url, title);
		}
		else
		{
			int size = strlen(escaped_url) + strlen("INSERT IGNORE INTO crawled (url) VALUES ('');") + 
				strlen(escaped_url) + strlen("UPDATE crawled SET frontier = 0 WHERE url = '';") +
				strlen(escaped_url) + strlen(escaped_title) + strlen("UPDATE crawled SET title = '' WHERE url = ''") + 1;
			sql_current->sql = (char *) malloc (size);
			memory += size;
			int ret = snprintf(sql_current->sql, size, "INSERT IGNORE INTO crawled (url) VALUES ('%s');UPDATE crawled SET frontier = 0 WHERE url = '%s';UPDATE crawled SET title = '%s' WHERE url = '%s'", escaped_url, escaped_url, escaped_title, escaped_url);
			if (ret >= 0 && ret <= size)
			{}
			else
			{
				if (debug)
					fprintf(stderr, "%s too big for buffer\n", escaped_url);
			}	
			sql_queue_inc();
			sql_current->next = NULL;
			if (debug)
				fprintf(stderr, "Inserting %s, Updating title %s\n", url, title);
		}
	}
	else
	{
		if (has_ads(html))
		{
			int size = strlen(escaped_url) + strlen("INSERT IGNORE INTO crawled (url) VALUES ('');") + 
				strlen(escaped_url) + strlen("UPDATE crawled SET frontier = 0 WHERE url = '';") +
				strlen(escaped_url) + strlen("UPDATE crawled SET ads = 1 WHERE url = ''") + 1;
			sql_current->sql = (char *) malloc (size);
			memory += size;
			int ret = snprintf(sql_current->sql, size, "INSERT IGNORE INTO crawled (url) VALUES ('%s');UPDATE crawled SET frontier = 0 WHERE url = '%s';UPDATE crawled SET ads = 1 WHERE url = '%s'", escaped_url, escaped_url, escaped_url);
			if (ret >= 0 && ret <= size)
			{}
			else
			{
				if (debug)
					fprintf(stderr, "%s too big for buffer\n", escaped_url);
			}	
			sql_queue_inc();
			sql_current->next = NULL;
			if (debug)
				fprintf(stderr, "Inserting %s, Setting ads = 1\n", url);
		}
		else
		{
			int size = strlen(escaped_url) + strlen("INSERT IGNORE INTO crawled (url) VALUES ('');") + strlen(escaped_url) + strlen("UPDATE crawled SET frontier = 0 WHERE url = ''") + 1;
			sql_current->sql = (char *) malloc (size);
			memory += size;
			int ret = snprintf(sql_current->sql, size, "INSERT IGNORE INTO crawled (url) VALUES ('%s');UPDATE crawled SET frontier = 0 WHERE url = '%s'", escaped_url, escaped_url);
			if (ret >= 0 && ret <= size)
			{}
			else
			{
				if (debug)
					fprintf(stderr, "%s too big for buffer\n", escaped_url);
			}	
			sql_queue_inc();
			sql_current->next = NULL;
			if (debug)
				fprintf(stderr, "Inserting %s\n", url);
		}
	}

	//html_link_find(url, html);
	html_mailto_find(html);
	if (title != NULL)
		memory -= strlen(title);
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
	printf("\rParsed sites: %d, %d parallel connections, %d still running, %d transfers, %d queued SQL queries, %d bytes allocated\t", 
			g->parsed_sites, g->concurrent_connections, g->still_running, g->transfers, sql_queue, memory);
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

			//memory += strlen(conn->url);
			//memory += strlen(conn->data);
			//memory += sizeof(conn);

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
			//memory -= strlen(conn->url);
			//memory -= strlen(conn->data);
			//memory -= sizeof(conn);
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
	memory += strlen(conn->url);
	memory -= strlen(url);
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
	curl_easy_setopt(conn->easy, CURLOPT_USERAGENT, "WoTBoT (http://wottot.com/WoTBoT.html)");

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
	memory += strlen(conn->url);
	//free(url);
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
	curl_easy_setopt(conn->easy, CURLOPT_USERAGENT, "WoTBoT (http://wottot.com/WoTBoT.html)");

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
		fprintf(stderr, "Starting crawler...\n");

	while (!should_exit) {
		int idx;
		int err = epoll_wait(g.epfd, events, sizeof(events)/sizeof(struct epoll_event), 10000);

		/* Pop url from frontier in crawled table and crawl it */
		if (sequence == ALL_ROWS_FETCHED)
		{
			if (debug)
				fprintf(stderr, "Doing select\n");
			status = mysql_real_query_nonblocking(mysql_conn, sql, (unsigned long)strlen(sql));

			if (status == NET_ASYNC_COMPLETE)
			{
				sequence = SELECT_DONE;
			} 
			else if (status == NET_ASYNC_ERROR)
			{
				if (mysql_errno(mysql_conn) == CR_SERVER_GONE_ERROR)
				{
					//mysql_stop();
					mysql_close(mysql_conn);
					//mysql_start();
					mysql_conn = mysql_init(NULL);
					if (mysql_conn == NULL)
        				{
        				        fprintf(stderr, "mysql_init %s\n", mysql_error(mysql_conn));
        				        exit(1);
       					}
					if (mysql_real_connect(mysql_conn, "localhost", "crawler", "1q2w3e4r", "crawl", 0, NULL, 0) == NULL)
					{
						fprintf(stderr, "mysql_real_connect %s\n", mysql_error(mysql_conn));
						mysql_close(mysql_conn);
						exit(1);
					}
				}
				else
				{
					fprintf(stderr, "mysql_real_query_nonblocking: %s errno: %d sql: %s\n",mysql_error(mysql_conn), mysql_errno(mysql_conn), sql);
					exit(EXIT_FAILURE);
				}
			}
		} 
		else if (sequence == SELECT_DONE) 
		{	
			if (debug)
				fprintf(stderr, "Select done. Storing result\n");
			result = mysql_use_result(mysql_conn);

                        if (result == NULL)
                        {
                                fprintf(stderr, "mysql_use_result: %s",mysql_error(mysql_conn));
                                exit(EXIT_FAILURE);
                        }
			else
			{
                                sequence = FETCHED_RESULT;
			}
		}
		else if (sequence == FETCHED_RESULT)
		{
			if (debug)
				fprintf(stderr, "Result stored. Fetching row\n");
			status = mysql_fetch_row_nonblocking(result, &row);

			if (status == NET_ASYNC_COMPLETE)
                        {
                                sequence = FETCHED_ROW;
				if (row == NULL)
				{
					if (debug)
						fprintf(stderr, "No more rows.\n");
					last_row_fetched = 1;
				}
				else
				{
					if (global_url != NULL)
						memory -= strlen(global_url);
					free(global_url);
					char *url = strdup(row[0]);
					memory += strlen(url);
					global_url = strdup(row[0]);
					memory += strlen(global_url);
					//global_url[strlen(row[0])] = '\0';
					if (debug)
						fprintf(stderr, "url: %s\n", url);
					new_head_conn(url, &g);
				}
                        }
                        else if (status == NET_ASYNC_ERROR)
                        {
                                fprintf(stderr, "mysql_fetch_row_nonblocking: %s",mysql_error(mysql_conn));
                                exit(EXIT_FAILURE);
                        }
		}
		else if (sequence == FETCHED_ROW)
		{
			if (global_url != NULL)
			{
				if (debug)
					fprintf(stderr, "Fetched row. Deleting url from frontier\n");
				char escaped_url[(strlen(global_url)*2)+1];
				if (!mysql_real_escape_string(mysql_delete_conn, escaped_url, global_url, strlen(global_url)))
				{}
				int size = strlen(escaped_url) + strlen("UPDATE crawled SET frontier = 0 WHERE url = ''") + 1;
				if (delete_sql != NULL)
					memory -= strlen(delete_sql);
				free(delete_sql);
				delete_sql = (char *) malloc (size);
				memory += size;
				int ret = snprintf( delete_sql, size, "UPDATE crawled SET frontier = 0 WHERE url = '%s'", escaped_url);
				if (ret >= 0 && ret <= size)
				{}
				else
				{
					if (debug)
						fprintf(stderr, "%s too big for buffer\n", escaped_url);
				}
				delete_status = mysql_real_query_nonblocking(mysql_delete_conn, delete_sql, (unsigned long)strlen(delete_sql));
				if (delete_status == NET_ASYNC_COMPLETE)
                       		{
					sequence = DELETE_DONE;
				}
				else if (delete_status == NET_ASYNC_ERROR)
                        	{
                        	        fprintf(stderr, "mysql_real_query_nonblocking: %s sql: %s\n",mysql_error(mysql_delete_conn), sql);
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
					if (current->sql != NULL)
						memory -= strlen(current->sql);
					free(current->sql);
					current->sql = strdup(sql_head->sql);
					memory += strlen(current->sql);
					memory -= strlen(sql_head->sql);
					memory -= sizeof(SqlNode);
					free(sql_head->sql);
					free(sql_head);		
					sql_queue_dec();
					sql_head = sql_tmp;
					//current->status = mysql_real_query_nonblocking(current->mysql_conn, current->sql, (unsigned long)strlen(current->sql));
					current->sequence = BUSY;
					current->sequence02 = INITIATE;
					if (debug)
						fprintf(stderr, "Query %s\n", current->sql);
				}
			}
			else if (current->sequence == BUSY)
			{
				if (current->sequence02 == INITIATE)
				{
					current->status = mysql_real_query_nonblocking(current->mysql_conn, current->sql, (unsigned long)strlen(current->sql));
					if (current->status == NET_ASYNC_COMPLETE)
					{
						current->sequence02 = STORE;
					}
					else if (current->status == NET_ASYNC_ERROR)
					{
						if (mysql_errno(current->mysql_conn) == CR_SERVER_GONE_ERROR)
						{
							//mysql_stop();
							mysql_close(current->mysql_conn);
							//mysql_start();
							current->mysql_conn = mysql_init(NULL);
							if (mysql_conn == NULL)
        						{
        						        fprintf(stderr, "mysql_init %s\n", mysql_error(current->mysql_conn));
        						        exit(1);
       							}
							if (mysql_real_connect(current->mysql_conn, "localhost", "crawler", "1q2w3e4r", "crawl", 0, NULL, 0) == NULL)
							{
								fprintf(stderr, "mysql_real_connect %s\n", mysql_error(current->mysql_conn));
								mysql_close(current->mysql_conn);
								exit(1);
							}
						}
						else
						{
							fprintf(stderr, "mysql_real_query_nonblocking: %s errno: %d sql: %s\n",mysql_error(current->mysql_conn), mysql_errno(current->mysql_conn), current->sql);
							exit(EXIT_FAILURE);
						}
					}
				}
				if (current->sequence02 == STORE)
				{
					current->status = mysql_store_result_nonblocking(current->mysql_conn, &current->result);
					if (current->status == NET_ASYNC_COMPLETE)
					{
						current->sequence02 = FREE;
						current->status = mysql_free_result_nonblocking(current->result);
					}
					else if (current->status == NET_ASYNC_ERROR)
					{
						fprintf(stderr, "mysql_store_result_nonblocking: %s sql: %s\n", mysql_error(current->mysql_conn), current->sql);
						exit(EXIT_FAILURE);
					}
				}
				else if (current->sequence02 == FREE)
				{
					current->status = mysql_free_result_nonblocking(current->result);
					if (current->status == NET_ASYNC_COMPLETE)
					{
						if (mysql_next_result(current->mysql_conn) == 0)
						{
							current->sequence02 = STORE;
							current->status = mysql_store_result_nonblocking(current->mysql_conn, &current->result);
						}	
						else
						{
							current->sequence = READY;
							current->sequence02 = DONE;
							if (debug)
								fprintf(stderr, "Done query %s\n", current->sql);
						}
					}
					else if (current->status == NET_ASYNC_ERROR)
					{
						fprintf(stderr, "mysql_free_result_nonblocking: %s sql: %s\n", mysql_error(current->mysql_conn), current->sql);
						exit(EXIT_FAILURE);
					}
				}
					/*
					current->sequence = READY;
					if (debug)
						fprintf(stderr, "Done query %s\n", current->sql);
					do
					{
						current->result = mysql_store_result(current->mysql_conn);
						mysql_free_result(current->result);
					} while (mysql_next_result(current->mysql_conn) == 0);
					*/
			}
			current = current->next;
		}

		if (err == -1) {
			if (errno == EINTR) {
				fprintf(MSG_OUT, "note: wait interrupted\n");
				continue;
			} else if (errno == EAGAIN) {
				perror("epoll_wait");
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
 
	memory -= strlen(global_url);
	memory -= strlen(delete_sql);
	free(global_url);
	free(delete_sql);

	if (sequence == FETCHED_RESULT || sequence == FETCHED_ROW || sequence == DELETE_DONE)
	{
		mysql_free_result(result);
	}

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
					if (current->sql != NULL)
						memory -= strlen(current->sql);
					free(current->sql);
					current->sql = strdup(sql_head->sql);
					memory -= strlen(sql_head->sql);
					memory -= sizeof(SqlNode);
					free(sql_head->sql);
					free(sql_head);
					sql_queue_dec();
					sql_head = sql_tmp;
					current->status = mysql_real_query_nonblocking(current->mysql_conn, current->sql, (unsigned long)strlen(current->sql));
					current->sequence = BUSY;
				}
			}
			else if (current->sequence == BUSY)
			{
				while (current->status = mysql_real_query_nonblocking(current->mysql_conn, current->sql, (unsigned long)strlen(current->sql)) == NET_ASYNC_NOT_READY);
					/* empty loop */
				if (current->status == NET_ASYNC_COMPLETE)
				{
					current->sequence = READY;
					if (debug)
						fprintf(stderr, "Done query %s\n", current->sql);
					do
					{
						current->result = mysql_store_result(current->mysql_conn);
						mysql_free_result(current->result);
					} while (mysql_next_result(current->mysql_conn) == 0);
				}
				else if (current->status == NET_ASYNC_ERROR)
				{
					fprintf(stderr, "mysql_real_query_nonblocking: %s sql: %s\n", mysql_error(current->mysql_conn), current->sql);
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

	while (sql_head != NULL)
	{
		SqlNode *sql_tmp = sql_head->next;
		if (mysql_real_query(mysql_conn, sql_head->sql, (unsigned long)strlen(sql_head->sql)))
		{
			fprintf(stderr, "mysql_real_query failed: %s", mysql_error(mysql_conn));
			//exit(EXIT_FAILURE);
		}
		memory -= strlen(sql_head->sql);
		memory -= sizeof(SqlNode);
		free(sql_head->sql);
		sql_queue_dec();
		free(sql_head);
		sql_head = sql_tmp;
	}

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
	mysql_library_end();


	printf("Exiting.\n");

	return (0);
}
