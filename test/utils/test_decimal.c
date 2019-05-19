/*
 * Description: 
 *     History: yang@haipo.me, 2017/06/05, create
 */

# include <stdio.h>
# include <string.h>

# include "ut_decimal.h"

int main(int argc, char *argv[])
{
    init_mpd();

    char *line = NULL;
    size_t buf_size = 0;
    while (getline(&line, &buf_size, stdin) != -1) {
        size_t len = strlen(line);
        if (len && line[len - 1] == '\n')
            line[--len] = 0;

        mpd_t *value = decimal(line, 0);
        if (value == NULL) {
            printf("not decimal\n");
            continue;
        }

        char buf[1024];
        printf("%s\n", strmpd(buf, sizeof(buf), value));
        mpd_del(value);
    }

    return 0;
}

