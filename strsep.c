#include <string.h>

char *strsep(char **stringp, const char *pattern) {
   char *ptr = *stringp;

   char *first = NULL;
   int len = strlen(pattern);

   for (int i = 0; i < len; ++i) {
      char *n = strchr(*stringp, pattern[i]);
      if (n != NULL && (first == NULL || n < first)) {
         first = n;
      }
   }

   if (first != NULL) {
      *first = '\0';
      *stringp = first + 1;
   } else {
      *stringp = NULL;
   }

   return ptr;
}
