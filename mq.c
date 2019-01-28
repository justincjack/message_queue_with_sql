#include "mq.h"



void sig_hand( int sgnl ) {
    printf("\nCTRL-C Caught...Shutting down...\n\n");
    ok_to_run = 0;
    return;
}


int start_listening( void ) {
    int s = 0, i = 0;
    int failed_attempts = 0;
    struct sockaddr_un sa;
    for (; i < 4; i++) {
        unlink(SOCKET_PATH);
        s = socket(AF_UNIX, SOCK_STREAM, 0);
        if (-1 == s) {
            printf("Failed to create socket descriptor\n");
            usleep(1000);
            continue;
        }
        memset(&sa, 0, sizeof(sa));
        sa.sun_family = AF_UNIX;
        sprintf(sa.sun_path, "%s", SOCKET_PATH);
        if (-1 == bind(s, (struct sockaddr *)&sa, sizeof(struct sockaddr_un))) {
            close(s);
            printf("mq.c: Call to bind() for listening socket failed.\n");
            usleep(1000);
            continue;
        }
        if (-1 == listen(s, 500)) {
            shutdown(s, 2);
            close(s);
            printf("mq.c > start_listening(): listen() system call failed.\n\n");
            usleep(1000);
            continue;
        }
        break;
    }
    if ( 5 == i ) return 0;
    chmod(SOCKET_PATH, 0777);
    return s;
}

int main(int argc, char **argv) {
    int i = 0, sel_ret = 0;
    int s_listener = 0, new_client = 0;
    struct sigaction sigact;
    time_t timeout = time(0) + 10;
    fd_set read_fds;
    struct timeval tv;
    struct sockaddr client;
    socklen_t socklen = 0;
    PIPC_CONNECTION cn = 0;


    /* Set up signal handlers */
    memset(&sigact, 0, sizeof(struct sigaction));
    sigact.sa_handler = &sig_hand;

    sigaction(SIGTERM, &sigact, NULL);
    sigaction(SIGINT, &sigact, NULL);


    s_listener = start_listening();
    if (!s_listener) {
        return 1;
    }
    printf("Message Queue is now listening...\n");
    /* Remember to lock the message mutex whenever we're manipulating messages
     * or we're using message manipulation functions!
     * 
     * pthread_mutex_lock(&message_mutex);
     * 
     */

    while (ok_to_run) {
        if (0 == s_listener) {
            printf("Connection broken...attempting to reestablish listening connection...\n");
            s_listener = start_listening();
            if (!s_listener) {
                ok_to_run = 0;
                break;
            }
        }
        FD_ZERO(&read_fds);
        FD_SET(s_listener, &read_fds);
        tv.tv_sec = 0;
        tv.tv_usec = 20000;
        sel_ret = select((s_listener+1), &read_fds, 0, 0, &tv);
        if (-1 == sel_ret) { // Error condition on socket.  Attempt recover.
            if (EINVAL == errno) {
                printf("The value within \"timeout\" was invalid.\n\n");
                ok_to_run = 0;
            }
            close(s_listener);
            s_listener = 0;
        } else if ( sel_ret > 0 ) {
            // Create a new IPC_CONNECTION here and pass it as a param to the thread.
            socklen = sizeof(struct sockaddr);
            new_client = accept(s_listener, &client, &socklen);
            if (new_client > 0) {
                if (!add_connection(new_client)) {
                    shutdown(new_client, 2);
                    close(new_client);
                }
                new_client = 0;
            } 
        }
    }

    if (s_listener > 0) {
        shutdown(s_listener, 2);
        close(s_listener);
    }
    printf("Exiting main thread...\n");
    /* Shut down all threads and sockets */
    pthread_mutex_lock(&connection_list_mutex);
    if (connection_list) {
        connection_list = connection_list->head;
        do {
            connection_list->okay_to_run = 0;
            if (!connection_list->next) break;
            connection_list = connection_list->next;
        } while (1);
    }
    pthread_mutex_unlock(&connection_list_mutex);

    printf("\nWaiting on all connections to shut down.\n");
    timeout = time(0) + 10;
    while (timeout > time(0)) {
        if (!connection_list) break;
        usleep(250);
    }
    printf("Exiting program....\n");
    return 0;
}

int add_connection( int socket ) {
    int retval = 0;
    PIPC_CONNECTION con = 0;
    if (!socket) return retval;
    con = calloc(sizeof(IPC_CONNECTION), 1);
    if (!con) return 0;
    
    con->is_active = 1;
    con->okay_to_run = 1;
    con->socket = socket;
    con->message_buffer = malloc(65536);
    if (!con->message_buffer) {
        free(con);
        return 0;
    }
    con->message_buffer_size = 65536;

    retval = 1;
    pthread_mutex_lock(&connection_list_mutex);
    if (!connection_list) {
        connection_list = con;
        con->head = con;
    } else {
        connection_list = connection_list->head;
        do {
            if (connection_list->next == 0) {
                connection_list->next = con;
                con->head = connection_list->head;
                con->previous = connection_list;
                break;
            }
            connection_list = connection_list->next;
        } while (1);
    }
    if ( 1 == retval ) {
        if (pthread_create(&con->thread, 0, &connection, con)) {
            remove_connection(con);
            free(con->message_buffer);
            free(con);
            retval = 0;
        } 
    }
    pthread_mutex_unlock(&connection_list_mutex);
    return retval;
}

PIPC_CONNECTION remove_connection(PIPC_CONNECTION connection) {
    PIPC_CONNECTION current = 0, head = 0, new_next = 0, new_head = 0;
    printf("remove_connection()\n");
    if (!connection) {
        printf("remove_connection(): Connection was NULL!\n");
    }
    if (!connection_list) {
        return 0;
    }
    new_next = connection->next;
    if (!new_next) {
        if (connection->previous) {
            new_next = connection->previous;
        } else {
            new_next = 0;
        }
    }
    head = connection->head;
    if (connection == head) { // This is the head element
        printf("This was the head element...\n");
        new_head = head->next;
        if (new_head) {
            printf("There are more elements...\n");
            current = new_head;
            new_head->previous = 0;
            do {
                current->head = new_head;
                current = current->next;
            } while (current);
            printf("Done re-linking the rest...\n");
        } else {
            printf("We're removing the last element\n");
            connection_list = 0;
            new_next = 0;
        }

    } else {
        printf("The element was not the head....\n");
        printf("Setting the NEXT of \"%s\" to %x\n", connection->previous->module_name, connection->next);
        connection->previous->next = connection->next;
        if (connection->next) {
            printf("There is a connection AFTER the one we're deleting...\n");
            printf("Setting the PREVIOUS of \"%s\" to %x\n", connection->next->module_name, connection->previous);
            connection->next->previous = connection->previous;
        }
    }
    if (connection->message_buffer) free(connection->message_buffer);
    free(connection);
    printf("remove_connection() returning: %x\n", new_next);
    return new_next;
}

void remove_messages_for(char *module) {
    if (!module) return;
    PCS_REQUEST rqp = 0;
    printf("remove_messages_for(\"%s\")\n", module);
    pthread_mutex_lock(&request_list_mutex);
    if (request_list) {
        rqp = request_list->head;
        do {
            if (!strcmp(rqp->target_module, module) ||
                    !strcmp(rqp->req_module, module)) {
                rqp = remove_request(rqp);
            }
            if (!rqp) break;
            rqp = rqp->next;
        } while (rqp);
    }
    pthread_mutex_unlock(&request_list_mutex);
    printf("exit remove_messages_for(\"%s\")\n", module);
    return;
}


int add_request(PCS_REQUEST request) {
    PCS_REQUEST r = calloc(1, sizeof(CS_REQUEST));
    if (!r) return 0;
    memcpy(r, request, sizeof(CS_REQUEST));
    pthread_mutex_lock(&request_list_mutex);
    // printf("Adding message to tail of the queue.\n\n");
    if (!request_list) {
        request_list = r;
        r->previous = 0;
        r->head = r;
        r->next = 0;
    } else {
        for (request_list = request_list->head; request_list->next; request_list = request_list->next);
        request_list->next = r;
        r->previous = request_list;
        r->head = request_list->head;
    }
    pthread_mutex_unlock(&request_list_mutex);
    return 1;
}

PCS_REQUEST remove_request(PCS_REQUEST request) {
    PCS_REQUEST current = 0, head = 0, new_next = 0;
    // pthread_mutex_lock(&request_list_mutex);
    if (!request) {
        // pthread_mutex_unlock(&request_list_mutex);
        return 0;
    }
    new_next = request->next;
    if (!new_next) {
        if (request->previous) {
            new_next = request->previous;
        } else {
            new_next = 0;
        }
    }
    head = request->head;
    // if (head != request) {
        // printf("*** This is not the HEAD element!\n");
    // }
    if (!request->previous) { // This is the head
        // printf("\tVerification that this IS isn't the head..\n");
        if (request->next) { /* If there's more than one request */
            head = request->next;
            head->previous = 0;
            for(current = head; current; current = current->next) {
                current->head = head;
            }
        } else {
            // printf("Zeroing out message queue!\n");
            request_list = 0;
        }
    } else {
        request->previous->next =request->next;
        if (request->next) {
            request->next->previous = request->previous;
        }
    }
    // pthread_mutex_unlock(&request_list_mutex);
    if (request) {
        if (request->szrequest) {
            free(request->szrequest);
        }
        free(request);
    } else {
        printf("* Possible error, \"request\" was null...\n");
    }
    // printf("remove_request(): returning %x\n", new_next);
    return new_next;
}

/* The thread that handles each incoming connection */
void *connection( void *param ) {
    int sel_ret = 0;
    size_t i = 0, j = 0;
    ssize_t bytes_read = 0, bytes_to_buffer = 0;
    size_t bytes_to_move = 0, bytes_buffered = 0, init_offset = 0, next_size = 0;
    ssize_t bytes_sent = 0, total_bytes_sent = 0, bytes_to_send = 0;
    size_t waiting_on_size = 0;
    time_t send_timeout = 0;
    int send_failed = 0;
    struct timeval tv;
    PIPC_CONNECTION connection = (PIPC_CONNECTION)param;
    char buffer[65536];
    char *swap = 0;
    fd_set fdr, fdw;
    time_t timeout = time(0) + 5, loop_pass = 0;
    int okay_to_process = 0;
    char module_name[500];


    memset(module_name, 0, 100);

    connection->okay_to_run = 1;

    if (param == 0) return 0;
    memset(buffer, 0, 63336);
    printf("New client connected.\n");
    while (connection->okay_to_run) {
        FD_ZERO(&fdr);
        FD_ZERO(&fdw);
        FD_SET(connection->socket, &fdr);
        FD_SET(connection->socket, &fdw);
        tv.tv_sec = 0;
        tv.tv_usec = 20000; 
        sel_ret = select( (connection->socket + 1), &fdr, 0, 0, &tv);
        if (-1 == sel_ret) {
            printf("select() error!\n");
            break;
        } else if ( sel_ret > 0) {
            if (FD_ISSET(connection->socket, &fdr)) {
                bytes_read = recv(connection->socket, buffer, 65536, 0);
                if ( bytes_read <= 0) {
                    break;
                }
                if (connection->waiting_on_bytes == 0 ) {
                    memcpy(connection->message_buffer, buffer, bytes_read);
                    connection->bytes_buffered+=bytes_read;
                    connection->waiting_on_bytes = handle_stream(connection);
                } else { /* Receiving the rest of a message */
                    okay_to_process = 1;
                    if (connection->bytes_buffered + bytes_read > connection->message_buffer_size) {
                        swap = realloc(connection->message_buffer, connection->bytes_buffered + bytes_read);
                        if (swap) {
                            connection->message_buffer_size =  connection->bytes_buffered + bytes_read;
                            connection->message_buffer = swap;
                            swap = 0;
                        } else {
                            okay_to_process = 0;
                        }
                    }
                    if (okay_to_process) {
                        memcpy(&connection->message_buffer[connection->bytes_buffered], buffer, bytes_read);
                        connection->bytes_buffered+=bytes_read;
                        connection->waiting_on_bytes = handle_stream(connection);
                        if (connection->waiting_on_bytes <= 0) {
                            connection->bytes_buffered = 0;
                            connection->waiting_on_bytes = 0;
                        }
                    } else { /* Failed to allocate enough memory */
                        connection->bytes_buffered = 0;
                        connection->waiting_on_bytes = 0;
                    }
                }
            }
        }   
        connection->last_message_check = time(0);
        pthread_mutex_lock(&request_list_mutex);
        if (request_list && connection->module_name[0] != 0) {
            request_list = request_list->head; 
            loop_pass = time(0);
            do {
                if ( (time(0) - request_list->request_start) > 30 ) {
                    request_list = remove_request(request_list);
                    if (!request_list) break;
                }
                total_bytes_sent = 0;
                if (!strcmp(request_list->target_module, connection->module_name)) {
                    if (request_list->szrequest) {
                        bytes_to_send = strlen(request_list->szrequest);
                        if (bytes_to_send > 0) {
                            send_timeout = time(0) + 4;
                            send_failed=0;
                            do {
                                if (time(0) > send_timeout) {
                                    printf("Send failed for \"%s\"\n", connection->module_name);
                                    send_failed = 1;
                                    break;
                                }
                                bytes_sent = send(connection->socket, 
                                    &request_list->szrequest[total_bytes_sent], 
                                    (bytes_to_send-total_bytes_sent),
                                    0);
                                if (bytes_sent == -1) {
                                    usleep(50000);
                                } else {
                                    total_bytes_sent+=bytes_sent;
                                }
                            } while (total_bytes_sent < bytes_to_send);
                        }
                    }
                    if (total_bytes_sent == bytes_to_send || send_failed == 0) {
                        request_list = remove_request(request_list);
                        connection->write_ready_after = time(0) + 10;
                        break;
                    }
                }
                if (!request_list) break; // Break if we removed the last request
                if (request_list->next == 0) break;
                request_list = request_list->next;
            } while (1);
        }
        pthread_mutex_unlock(&request_list_mutex);
        if (time(0) > timeout) {
            timeout = time(0) + 5;
        }
    }
    sprintf(module_name, "%s", connection->module_name);
    printf("Exiting connection thread for module \"%s\"\n", ((strlen(connection->module_name) == 0)?"UNDEFINED":connection->module_name));
    connection->is_active = 0;
    shutdown(connection->socket, 2);
    close(connection->socket);
    connection->socket = 0;

    printf("Removing connection for \"%s\"\n", ((strlen(connection->module_name) == 0)?"UNDEFINED":connection->module_name));
    pthread_mutex_lock(&connection_list_mutex);
    connection_list = remove_connection(connection);
    pthread_mutex_unlock(&connection_list_mutex);

    printf("Removing messages for \"%s\"\n", module_name);
    remove_messages_for(module_name);
    return 0;
}

ssize_t handle_stream( PIPC_CONNECTION connection) {
    size_t new_message_start_offset = 0, new_message_size = 0, bytes_to_move = 0;
    ssize_t bytes;
    bytes = connection->bytes_buffered;
    new_message_start_offset = find_next_message(connection, &new_message_size);
    if (new_message_size > 0) {
        if (new_message_size <= connection->bytes_buffered) {
            handle_message(connection, &connection->message_buffer[new_message_start_offset], new_message_size);
            connection->bytes_buffered-=(new_message_size+new_message_start_offset);
            if ( connection->bytes_buffered > 0) {
                memmove(connection->message_buffer, &connection->message_buffer[new_message_size+new_message_start_offset], connection->bytes_buffered);
                return handle_stream(connection);
            }
            return 0;
        }
    } 
    return (new_message_size+new_message_start_offset);
}

ssize_t find_next_message( PIPC_CONNECTION connection, size_t *data_length) {
    ssize_t i = 0, j = 0, start_offset = 0, size_search_offset = 0;
    ssize_t length = 0;
    char *start_position = 0;
    char *end_position = 0;
    size_t possible_data_length = -1;
    if (!data_length) return -1;
    length = connection->bytes_buffered;
    *data_length = 0;
    start_position = (char *)strnstr(connection->message_buffer, (char *)"message_length:", (size_t)length);
    if (!start_position) return 0;
    if (start_position != connection->message_buffer) { /* There was garbage in front of "message_length:" */
        start_offset = (start_position - connection->message_buffer);
        connection->bytes_buffered = (length-=start_offset);
        memmove(connection->message_buffer, start_position, length);
        start_offset = find_next_message(connection, data_length);
        return start_offset;
    } else { /* The "message_length:" was at the beginning */
        start_offset = 0;
        start_position = connection->message_buffer;
        size_search_offset = start_offset+15;
        for (i = size_search_offset; i < connection->bytes_buffered; i++) {
            if (!IS_DIGIT(connection->message_buffer[i])) {
                if (connection->message_buffer[i] == ':') { /* The proper character to find next. */
                    end_position = &connection->message_buffer[(i-1)];
                    possible_data_length = (size_t)strtoul(&connection->message_buffer[size_search_offset], &end_position, 10);
                    if (possible_data_length > 0) { /* There is data */
                        *data_length = possible_data_length;
                        return (i+1);
                    } else { /* There's zero bytes  */
                        if (i == connection->bytes_buffered) { /*  If the place where the number representing the length of the 
                                                        message should be is the end of the data */
                            connection->bytes_buffered = 0;
                            return -1;
                        } else {
                            length = (connection->bytes_buffered - (i+1));
                            memmove(connection->message_buffer, &connection->message_buffer[(i+1)], length);
                            connection->bytes_buffered = length;
                            start_offset = find_next_message(connection, data_length);
                            return start_offset;
                        }
                    }
                } else {
                    /* Protocol error. Drop this packet */
                    connection->bytes_buffered = 0;
                    return 0;
                }
                break;
            }
        }
    }
}

void *sql_query_thread( void *param ) {
    PSQL_REQUEST sr = (PSQL_REQUEST) param;
    PCS_REQUEST rq = 0;
    sqlite3 *sqlconn = 0;
    char *sqlerror = 0, *colname = 0, *colval = 0;
    int r = 0, i = 0, j = 0;
    int attempts = 0;
    size_t json_mem_req = 0;
    time_t timeout = time(0) + 10; /* 10 second timeout on database connection */
    const char unknown_error[] = "An unexplainable error occured trying to process your SQL query.";

    if (!param) return 0;

    printf("Trying to open SQLite Database...\n");
    do {
        if (!(r=sqlite3_open(DATABASE_PATH, &sqlconn))) break;
        usleep(10000);
    } while (time(0) > timeout);

    if (r) {
        if (sr->response_module) free(sr->response_module);
        if (sr->sql_query) free(sr->sql_query);
        printf("Failed to open SQLite database!\n\n");
        return 0;
    }
    printf("Executing: \"%s\"\n", sr->sql_query);
    for (attempts = 0; attempts < 20; attempts++) {
        r = sqlite3_exec(sqlconn, sr->sql_query, sqlite_callback, sr, &sqlerror); /* The result will be put into the request queue by the thread */
        if (r == SQLITE_OK) break;
        if (sqlerror) {
            if (!strstr(sqlerror, "database is locked")) break;
        }
        printf("Database locked, sleeping...\n");
        usleep(10000);
    }
    printf("Done with query...\n");
    if (r != SQLITE_OK) { /* SQL Error */
        if (!sqlerror) {
            sqlerror = (char *)unknown_error;
        }
        sr->result = calloc(1, strlen(sqlerror) + 100);
        sprintf(sr->result, "mq_message_num=%zu&%s", sr->message_number, sqlerror);
        printf("Sending SQLite error: %s\n\n", sr->result);
    } else { /* SQL Success */
        // printf("The query was successful, we need to deal with the variable \"sr\"!\n");
        if (sr->rows_filled > 0) {
            size_t string_size = 0;
            char *field = 0;

            json_mem_req = (((sr->header_size_needed * sr->rows_filled) + sr->json_size_needed) + 100 /* For the message number data */);
            sr->result = calloc(json_mem_req, 1);
            sprintf(sr->result, "mq_message_num=%zu&", sr->message_number);
            // printf("Result is now: %s\n", sr->result);
            string_size = strlen(sr->result);
            sr->result[string_size++] = '[';
            for (i = 0; i < sr->rows_filled; i++) {
                if (i > 0) {
                    sr->result[string_size++] = ',';
                }
                sr->result[string_size++] = '{';
                for (j = 0; j < sr->ncols; j++) {
                    if ( j > 0) {
                        sr->result[string_size++] = ',';
                    }
                    sr->result[string_size++] = '\"';
                    field = ((sr->col_names[j])?sr->col_names[j]:"No Name");
                    strcpy(&sr->result[string_size], field);
                    string_size+=strlen(field);
                    sr->result[string_size++] = '\"';
                    sr->result[string_size++] = ':';
                    sr->result[string_size++] = '\"';
                    field = ((sr->rows[i].cols[j])?sr->rows[i].cols[j]:"");
                    strcpy(&sr->result[string_size], field);
                    string_size+=strlen(field);
                    sr->result[string_size++] = '\"';
                }
                sr->result[string_size++] = '}';
            }
            sr->result[string_size++] = ']';

            for (i = 0; i < sr->ncols; i++) {
                if (sr->col_names[i]) free(sr->col_names[i]);
            }
            for (i = 0; i < sr->rows_filled; i++) {
                for (j = 0; j < sr->rows[i].ncols; j++) {
                    if (sr->rows[i].cols[j]) {
                        free(sr->rows[i].cols[j]);
                        sr->rows[i].cols[j] = 0;
                    }
                }
            }
            if (sr->rows) {
                free(sr->rows);
                sr->rows = 0;
                sr->rows_filled = 0;
                sr->rows_allocated = 0;
            }
            // printf("Size of JSON object: %zu bytes!\n", strlen(sr->result));
            // printf("%zu bytes needed to hold JSON object\n\n", json_mem_req);
            // printf("JSON Object is ready to send!\n\n");
        } else {
            sr->result = malloc(8);
            strcpy(sr->result, "success");
        }

    }

    if (sr->result) { /* We have a result to send back */
        rq = (PCS_REQUEST)calloc(1, sizeof(CS_REQUEST));
        if (rq) {
            char size_buffer[100];
            char from_buffer[1000];
            size_t from_buffer_len = 0;
            char *b64databuffer = 0;

            memset(size_buffer, 0, 100);
            memset(from_buffer, 0, 1000);
            sprintf(rq->req_module, "%s", sr->response_module);
            sprintf(rq->target_module, "%s", sr->response_module);
            rq->request_start = time(0);
            sprintf(from_buffer, "mq_type=response&mq_from=%s&data=", sr->response_module);
            from_buffer_len = strlen(from_buffer);
            base64encode(sr->result, strlen(sr->result), &b64databuffer);
            if (b64databuffer) {
                // printf("Data is base64 encoded!\n");
                from_buffer_len+=strlen(b64databuffer);
                sprintf(size_buffer, "%zu:", from_buffer_len);
                // printf("Size buffer is now: \"%s\"\n", size_buffer);

                // printf("Building request!\n");
                rq->szrequest = calloc((from_buffer_len + strlen(size_buffer) + 100), 1);
                sprintf(rq->szrequest, "%s%s%s", size_buffer, from_buffer, b64databuffer);
                // printf("Request so far: \"%s\"\n", rq->szrequest);
                rq->szrequest_len = strlen(rq->szrequest);
                // printf("Freeing base64 buffer!\n");
                free(b64databuffer);
            } else {
                printf("**** Failed to Base64 Encode Data!\n");
            }
            // printf("Freeing other stuff\n");
            free(sr->result);
            if ( rq->szrequest) {
                add_request(rq);
            }
            free(rq); /* Don't free rq->szrequest, it will be freed later */
        }
    }

    sqlite3_close(sqlconn);
    return 0;
}

/* Launches thread to handle query */
void handle_sql_query( size_t message_number, char *to_module, size_t to_module_len, char *sql_query, size_t sql_query_len) {
    PSQL_REQUEST sq = calloc(1, sizeof(SQL_REQUEST));
    pthread_t sqt;

    printf("\n\nhandle_sql_query()\n");

    if (!sq) {
        printf("Failed to allocate memory for SQL request!\n");
        return;
    }

    sq->response_module = calloc((to_module_len + 1) ,1);
    if (!sq->response_module) {
        printf("Failed to allocate memory to put module name\n");
        free(sq);
        return;
    }
    sq->sql_query = calloc((sql_query_len + 1), 1);
    if (!sq->sql_query) {
        printf("Failed to allocate memory for the query\n");
        free(sq->response_module);
        free(sq);
        return;
    }
    memcpy(sq->response_module, to_module, to_module_len);
    memcpy(sq->sql_query, sql_query, sql_query_len);
    sq->message_number = message_number;
    // printf("Creating thread for module \"%s\" and query \"%s\"\n", sq->response_module, sq->sql_query);
    pthread_create(&sqt, 0, sql_query_thread, sq);
}


/* Handle the message and put a message on the stack if warranted */
int handle_self_message( PIPC_CONNECTION connection, PCS_REQUEST req) {
    char *data_to_decode = 0, *decoded_data = 0, *encoded_data = 0;
    char *msg_num_start = 0, *msg_num_end = 0 , *msg_num = 0, *query = 0;
    char *number_start = 0;
    size_t amp_offset = 0, i = 0, decoded_len = 0, message_number = 0;
    data_to_decode = strstr(req->szrequest, "data=");
    if (data_to_decode) {
        data_to_decode = &data_to_decode[5];
        if ( -1 != base64decode(data_to_decode, strlen(data_to_decode), &decoded_data)) {
            // printf("Processing decoded data \"%s\" (%zu bytes)\n", decoded_data, strlen(decoded_data));
            /* 
            Here the data will contain a string like the following:
                - mq_message_num=275&SELECT * FROM item;

            We need to parse out the "mq_message_num" to get to the SQL
            query.  When we put everything back together, we need to add
            in the mq_message_num field along with the data AFTER the
            ampersand.

            */
            msg_num_start = strstr(decoded_data, MSG_NUM_PATTERN);
            if (msg_num_start) {
                // printf("Found msg_num_start at: 0x%x\t\"%s\"\n\n", msg_num_start, msg_num_start);
                // printf("sizeof(MSG_NUM_PATTERN)=%zu\n\n", strlen(MSG_NUM_PATTERN));
                
                msg_num = &msg_num_start[strlen(MSG_NUM_PATTERN)];
                // printf("\n\nStarting at: %s\n\n", msg_num);

                for (i = 0; 0 != msg_num[i]; i++) {
                    // printf("%c", msg_num[i]);
                    if (msg_num[i]=='&') {
                        // printf("\n! Found Ampersand!\n");
                        msg_num_end = &msg_num[(i-1)];
                        query = &msg_num[(i+1)];
                        amp_offset = i;
                        break;
                    }
                }
                // printf("\n");
                if (msg_num_end && query) {
                    // printf("Number starts with \"%c\", and ends with \"%c\"\n", msg_num[0], msg_num_end[0]);
                    message_number = strtoul(msg_num, &msg_num_end, 10);
                    // printf("Message Number: %zu\n", message_number);
                    handle_sql_query(message_number, connection->module_name, strlen(connection->module_name), query, strlen(query));
                }
            }
            free(decoded_data);
            decoded_data = 0;
        }
    }
    return 0;
}

/* The function receives a message, parses it, and places it in the queue of the
 * target module for processing.
 */
int handle_message( PIPC_CONNECTION connection, char *message, size_t length) {
    int i = 0, j = 0;
    char *p = 0;
    CS_REQUEST req;
    PCS_REQUEST r = 0;
    char size_buffer[100];
    char from_appendage[500];
    size_t size_buffer_len = 0;
    if (!message || !length) return 0;
    memset(&req, 0, sizeof(CS_REQUEST));
    /* Message structure to parse
     * MQ_TEST:WEBSOCKET:This is a test!
     *    ^        ^     ^
     *    |        |     |___This is the message we'll send to the target module.
     *    |        |
     *    |        |___This is the target module for which the message is intended
     *    |
     *    |__ This first bit is the name we'll save to "connection->module_name" if it's empty
    */

    // printf("Parsing \"%.*s\" length=%d bytes\n", length, message, length);
    p = req.req_module;
    for (; i < length; i++) {
        if (message[i] == ':') {
            if (p == req.req_module) {
                p = req.target_module;
                j = 0;
            } else { // (i+1) is the start of the message;
                i++;
                if (i < length) {
                    memset(size_buffer, 0, sizeof(size_buffer));
                    memset(from_appendage, 0, sizeof(from_appendage));
                    sprintf(from_appendage, "mq_from=%s&", lcase(req.req_module));
                    // sprintf(size_buffer, "%zu:", (length-i) + strlen(from_appendage), req.req_module);
                    sprintf(size_buffer, "%zu:", (length-i) + strlen(from_appendage));
                    size_buffer_len = strlen(size_buffer) + strlen(from_appendage);
                    req.szrequest_len = (length - i) + size_buffer_len + 1;
                    req.szrequest = malloc(req.szrequest_len);
                    memset(req.szrequest, 0, req.szrequest_len);
                    sprintf(req.szrequest, "%s%s", size_buffer, from_appendage);
                    // printf("Moving \"%.*s\" (%zu bytes) into the data buffer...\n", (length-i), &message[i], req.szrequest_len );
                    memcpy(&req.szrequest[size_buffer_len], &message[i], (length - i));
                }
                break;
            }
        } else {
            if ( j < 100) p[j++] = message[i];
        }
    }
    connection->write_ready_after = 0;
    req.request_start = time(0);
    lcase(req.target_module);
    lcase(req.req_module);

    // printf("\nMessage Received.\n");
    // printf("From: %s\n", req.req_module);
    // printf("To: %s\n", req.target_module);
    // if (req.szrequest) printf("%zu bytes.\n", strlen(req.szrequest));
    // if (req.szrequest) {
    //     printf("Message: \"%.*s\"\n\n", req.szrequest_len, req.szrequest);
    // }

    if (strlen(req.target_module) == 0 || strlen(req.req_module) == 0) {
        printf("** Unnamed module communique! Not processing this message!\n");
        if (req.szrequest) free(req.szrequest);
        return 1;
    }

    if (connection->module_name[0] == 0) {
        printf("Setting module name for this connection as \"%s\"\n", req.req_module);
        strcpy(connection->module_name, req.req_module);
    }

    if (!strcmp(req.target_module, req.req_module)) {
        if (req.szrequest) {
            printf("calling handle_self_message()\n");
            handle_self_message(connection, &req);
            free(req.szrequest);
            return 1;
        }
        return 1; // Don't process a message to ourself...
    }
    // printf("Saving this message to the queue...\n");
    return ((req.szrequest)?add_request(&req):1);
}

#ifndef strnstr
char *strnstr( char *haystack, char *needle, size_t length) {
    size_t i = 0, needle_length = 0;
    if (!haystack || !needle) return 0;
    needle_length = strlen(needle);
    while ( (i+needle_length) <= length ) {
        if (!strncmp(&haystack[i], needle, needle_length)) return &haystack[i];
        i++;
    }
    return 0;
}
#endif


char *json_escape( char *input, size_t input_length, size_t *json_length) {
    int i = 0, j = 0;
    size_t new_length = 0;
    if (json_length) {
        *json_length = 0;
    }
    char *escaped = 0;
    for (i = 0; i < input_length; i++) {
        if (input[i] == '\b') {
            new_length+=2;
        } else if (input[i] == '\n') {
            new_length+=2;
        } else if (input[i] == '\r') {
            new_length+=2;
        } else if (input[i] == 12) {
            new_length+=2;
        } else if (input[i] == '\t') {
            new_length+=2;
        } else if (input[i] == '\"') {
            new_length+=2;
        } else if (input[i] == '\\') {
            new_length+=2;
        } else if (input[i] == '/') {
            new_length+=2;
        } else if (input[i] == '\f') {
            new_length+=2;
        } else {
            new_length++;
        }
    }
    new_length++;
    escaped = calloc(1, new_length);
    if (json_length) {
        *json_length = (new_length - 1);
    }
    if (!escaped) return input;
    for (i = 0, j = 0; i < input_length; i++, j++) {
        if (input[i] == '\b') {
            escaped[j++] = '\\';
            escaped[j] = 'b';
        } else if (input[i] == '\n') {
            escaped[j++] = '\\';
            escaped[j] = 'n';
        } else if (input[i] == '\r') {
            escaped[j++] = '\\';
            escaped[j] = 'r';
        } else if (input[i] == '\t') {
            escaped[j++] = '\\';
            escaped[j] = 't';
        } else if (input[i] == '\"') {
            escaped[j++] = '\\';
            escaped[j] = '\"';
        } else if (input[i] == '\\') {
            escaped[j++] = '\\';
            escaped[j] = '\\';
        } else if (input[i] == '/') {
            escaped[j++] = '\\';
            escaped[j] = '/';
        } else if (input[i] == '\f') {
            escaped[j++] = '\\';
            escaped[j] = 'f';
        } else {
            escaped[j] = input[i];
        }
    }
    return escaped;
}

/* Called for each row */
static int sqlite_callback( void *data, int argc, char **argv, char **col_names) {
    PSQL_REQUEST sr = (PSQL_REQUEST)data;
    int col = 0;
    size_t temp_len = 0, json_length = 0;
    PSQL_ROW row;
    PSQL_ROW swap;

    // printf("In callback...\n");
    if ( 0 == sr->rows_allocated) {
        // printf("\tAllocating ROWS_TO_ALLOCATE rows...\n");
        sr->rows = (PSQL_ROW)calloc(ROWS_TO_ALLOCATE, sizeof(SQL_ROW));
        if (sr->rows) {
            sr->rows_allocated = ROWS_TO_ALLOCATE;
            // printf("\tDone %d rows allocated for starters\n\n", sr->rows_allocated);
        } else {
            return 1;
        }
    }


    if (sr->rows_filled + 1 == sr->rows_allocated) {
        // printf("\tOut of row space, allocating more!\n");
        swap = (PSQL_ROW)realloc(sr->rows, ((sr->rows_allocated + ROWS_TO_ALLOCATE)  * sizeof(SQL_ROW))   );
        if (swap) {
            sr->rows_allocated+=ROWS_TO_ALLOCATE;
            sr->rows = swap;
            swap = 0;
        } else {
            printf("*** FAILED TO ALLOCATE MORE MEMORY in sqlite_callback()\n");
            return 1;
        }
    } 
    // printf("\tSetting our row to fill\n");
    row = &sr->rows[sr->rows_filled];

    // printf("\tSetting the column number to %d\n", argc);
    row->ncols = argc;
    // printf("\tAllocating space for %d collumns.\n", argc);
    row->cols = calloc(argc, sizeof(char *));

    if (!row->cols) {
        printf("\tFailed to allocate memory for our columns!\n");
        return 1;
    }

    /* Fill the column names if this is the first row*/
    if ( 0 == sr->rows_filled) {
        // printf("\tAllocating pointer space for our column names!\n");
        sr->ncols = argc;
        sr->col_names = (char **)calloc(argc, sizeof(char *));
        if (!sr->col_names) {
            printf("\tcalloc() failed for column names!\n");
            return 1;
        }
        // printf("\tFilling our column names\n\t");
        for (col = 0; col < argc; col++) {
            if (col_names[col]) {
                temp_len = strlen(col_names[col]);
                sr->col_names[col] = json_escape(col_names[col], temp_len, &json_length);
                sr->header_size_needed+=json_length;
            }
        }
        printf("\n\n");
    }
    
    /* Copy column values */
    sr->json_size_needed+=3; /* Account for the {}, */
    for (col = 0; col < argc; col++) {
        sr->json_size_needed+=6;
        if (argv[col]) {
            temp_len = strlen(argv[col]);
            row->cols[col] = json_escape(argv[col], temp_len, &json_length);
            sr->json_size_needed+=json_length;
        } 
    }
    sr->rows_filled++;
    return 0;
}