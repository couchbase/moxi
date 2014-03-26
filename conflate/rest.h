#ifndef REST_H
#define	REST_H

#define RESPONSE_BUFFER_SIZE 4096
#define END_OF_CONFIG "\n\n\n\n"
#define CONFIG_KEY "contents"
#define HTTP_CODE_KEY "http_code"

void run_rest_conflate(void *arg);

#endif	/* REST_H */

