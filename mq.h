/*
 * mq.h - Message Queue
 * This is the message queue module for Cyber Server.  The purpose of this 
 * program is to broker requests between modules.  This program will listen
 * on a UNIX (IPC) socket and queue messages.  It will dispatch the messages
 * to modules in the request and return the data back to the requesting
 * module when the message has been serviced.
 * 
 * 
 * https://github.com/justincjack/message_queue.git
*/
#define _GNU_SOURCE
#include <stdio.h>
#include <time.h>
#include <pthread.h>
#include <errno.h>
#include <memory.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <sqlite3.h>
#include <unistd.h>
#include <sys/un.h>
#include <sys/types.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <sys/stat.h>

#include "b64.h"


#define REQUEST_TIMEOUT(request) ((time(0)>(request->request_start+60))?1:0)
#define IS_DIGIT(x) ((x>='0' && x <='9')?1:0)

#define SOCKET_PATH "../sockets/mq.sock\0"
#define DATABASE_PATH "../data/cyberserver.db\0"

#define MSG_NUM_PATTERN "mq_message_num="

#define ROWS_TO_ALLOCATE 200

/* Forward declarations */
typedef struct cs_request_list REQUEST_LIST, *PREQUEST_LIST;
typedef struct ipc_connection IPC_CONNECTION, *PIPC_CONNECTION;
typedef struct cs_request CS_REQUEST, *PCS_REQUEST;


static int ok_to_run = 1;

typedef struct sql_row {
    char **cols;
    int ncols;
} SQL_ROW, *PSQL_ROW;

/* Structure passed to thread that handles the SQL */
typedef struct sql_request {
    size_t message_number;
    char *sql_query;
    char *response_module;
    char **col_names;
    int ncols; /* Number of columns */
    PSQL_ROW rows;
    size_t header_size_needed;
    size_t size_needed;
    size_t json_size_needed;
    int rows_allocated;
    int rows_filled;
    char *result;
} SQL_REQUEST, *PSQL_REQUEST;

/* Mutex protecting the linked-list for requests to which all thread have
 * access.
 */
// static pthread_mutex_t request_list_mutex = PTHREAD_RECURSIVE_MUTEX_INITIALIZER_NP;
static pthread_mutex_t request_list_mutex = PTHREAD_MUTEX_INITIALIZER; 

/* Mutex protecting the linked-list of connections to which all threads 
 * have access
*/
// static pthread_mutex_t connection_list_mutex = PTHREAD_RECURSIVE_MUTEX_INITIALIZER_NP;
static pthread_mutex_t connection_list_mutex = PTHREAD_MUTEX_INITIALIZER;


/* A struct representing a module's connection to the message queue.  Modules will
 * be responsible for maintaining their connection to the message queue and will
 * reestablish connectivity if it's lost.  The message queue will keep any messages
 * queued for any particular connection until either they're delivered successfully
 * or they've timed out (after one minute).
 * 
 * IPC_CONNECTION objects will be replaced if a module re-connects.  Only one
 * connection per module is allowed.
 * 
*/
struct ipc_connection {
    int socket;                 // The UNIX socket descriptor

    char module_name[100];      // The name of the module that's connected.

    pthread_t thread;           // A handle to the thread handling this connection

    int okay_to_run;            // Flag that is initialized to 1 and set to 0 when a
                                // connection thread should die.

    int is_active;              // Flag indicating if the connection is active and has its thread running.

    char *message_buffer;

    size_t message_buffer_size;

    size_t bytes_buffered;

    size_t waiting_on_bytes;

    time_t last_message_check;

    int write_ready_after;      // The timeout after we will assume we're write-ready

    PREQUEST_LIST connection_request_list;

    sqlite3 *db;                // SQLite3 Connection

    char *sql_error;            // SQLite3 Error Buffer


    PIPC_CONNECTION head, next, previous;
};


/* Basic structure of a request.  Requests will time out after one minute */
struct cs_request {

    time_t request_start;       // The time-stamp of when the request was received

    char req_module[100];       // The module that issued the request

    char target_module[100];    // The module for which the request is intended

    char *szrequest;            // The string request

    size_t szrequest_len;       // The length of the data in szrequest

    PCS_REQUEST head, next,     // Linked list
        previous;    
};

/* The master linked list for all requests */
static PCS_REQUEST request_list = 0;
static PIPC_CONNECTION connection_list = 0;


int start_listening( void );


/* find_next_message()
 *
 * This function scans a buffer for the first legitimate message
 * send from a CyberServer module.
 *
 *
 * param: { char * }  buffer - The buffer to scan for messages
 * param: { size_t }  length - The length (in octets) of the data in the buffer
 * param: { size_t *} data_length - A pointer to a "size_t" variable into which the length of the message is returned
 * 
 * returns: { size_t } The offset (in octets) of the start of the message.
 * 
*/
ssize_t find_next_message( PIPC_CONNECTION connection, size_t *data_length);



ssize_t handle_stream( PIPC_CONNECTION connection);

int handle_self_message( PIPC_CONNECTION connection, PCS_REQUEST req);

int add_connection( int socket );

void remove_messages_for(char *module);

PIPC_CONNECTION remove_connection( PIPC_CONNECTION connection);


int add_request(PCS_REQUEST request);

PCS_REQUEST remove_request(PCS_REQUEST request);

/* Attempt to deliver the request to the module for which it was intended */
int dispatch_request(PCS_REQUEST);

/* This function scans the master request list "request_list", and builds
 * the linked list for a specific connection.  This will be called each time
 * a new module connects to the message queue program. 
 * 
 * The main purpose of this is that if a module drops its connection and re-connects,
 * any dangling requests for this module are put back into the list for the
 * new connection.
*/
int build_connection_request_list(PIPC_CONNECTION connection);

/* Thread to handle the connection */
void *connection( void *param );

static int sqlite_callback( void *data, int argc, char **argv, char **col_names);

char *json_escape( char *input, size_t input_length, size_t *json_length );

char *lcase( char *string ) {
    int i = 0;
    for (i = 0; i < strlen(string); i++) {
        string[i] = ((string[i] >= 'A' && string[i] <= 'Z')?(string[i] + 32):string[i]);
    }
    return string;
}

#ifndef strnstr
char *strnstr( char *haystack, char *needle, size_t length);
#endif
int handle_message( PIPC_CONNECTION connection, char *message, size_t length);