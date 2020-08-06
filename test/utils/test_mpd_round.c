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
    char buf[20] = {0};
    mpd_t *a = mpd_new(&mpd_ctx);
    mpd_t *b = mpd_new(&mpd_ctx);
    mpd_t *c = mpd_new(&mpd_ctx);

    mpd_t *interval = mpd_new(&mpd_ctx);

    printf("round up\n");
    mpd_set_string(a, "234.5563", &mpd_ctx);
    mpd_set_string(b, "234.5567", &mpd_ctx);
    mpd_set_string(c, "234.5560", &mpd_ctx);

    printf("a: %s\n", strmpd(buf, sizeof(buf), a));
    printf("b: %s\n", strmpd(buf, sizeof(buf), b));
    printf("c: %s\n", strmpd(buf, sizeof(buf), c));

    mpd_set_round_up();
    mpd_set_string(interval, "0.001", &mpd_ctx);
    mpd_quantize(a, a, interval, &mpd_ctx);
    mpd_quantize(b, b, interval, &mpd_ctx);
    mpd_quantize(c, c, interval, &mpd_ctx);
    
    printf("a: %s\n", strmpd(buf, sizeof(buf), a));
    printf("b: %s\n", strmpd(buf, sizeof(buf), b));
    printf("c: %s\n", strmpd(buf, sizeof(buf), c));

    printf("round down:\n");
    mpd_set_string(a, "234.5563", &mpd_ctx);
    mpd_set_string(b, "234.5567", &mpd_ctx);
    mpd_set_string(c, "234.5560", &mpd_ctx);

    printf("a: %s\n", strmpd(buf, sizeof(buf), a));
    printf("b: %s\n", strmpd(buf, sizeof(buf), b));
    printf("c: %s\n", strmpd(buf, sizeof(buf), c));

    mpd_set_round_down();
    mpd_set_string(interval, "0.001", &mpd_ctx);
    mpd_quantize(a, a, interval, &mpd_ctx);
    mpd_quantize(b, b, interval, &mpd_ctx);
    mpd_quantize(c, c, interval, &mpd_ctx);
    
    printf("a: %s\n", strmpd(buf, sizeof(buf), a));
    printf("b: %s\n", strmpd(buf, sizeof(buf), b));
    printf("c: %s\n", strmpd(buf, sizeof(buf), c));

    printf("round down:\n");
    mpd_set_string(a, "254.5563", &mpd_ctx);
    mpd_set_string(b, "264.5567", &mpd_ctx);
    mpd_set_string(c, "274.5560", &mpd_ctx);

    printf("a: %s\n", strmpd(buf, sizeof(buf), a));
    printf("b: %s\n", strmpd(buf, sizeof(buf), b));
    printf("c: %s\n", strmpd(buf, sizeof(buf), c));

    mpd_set_round_up();
    mpd_set_string(interval, "1", &mpd_ctx);
    mpd_quantize(a, a, interval, &mpd_ctx);
    //mpd_scaleb(a, a, interval, &mpd_ctx);
    mpd_quantize(b, b, interval, &mpd_ctx);
    mpd_quantize(c, c, interval, &mpd_ctx);
    
    printf("a: %s\n", strmpd(buf, sizeof(buf), a));
    printf("b: %s\n", strmpd(buf, sizeof(buf), b));
    printf("c: %s\n", strmpd(buf, sizeof(buf), c));

    return 0;
}

