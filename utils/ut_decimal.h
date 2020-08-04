/*
 * Description: 
 *     History: yang@haipo.me, 2017/03/17, create
 */

# ifndef _UT_DECIMAL_H_
# define _UT_DECIMAL_H_

# include <mpdecimal.h>
# include <jansson.h>

extern mpd_context_t mpd_ctx;

extern mpd_t *mpd_one;
extern mpd_t *mpd_ten;
extern mpd_t *mpd_zero;

int init_mpd(void);
int mpd_set_round_up(void);
int mpd_set_round_down(void);
mpd_t *decimal(const char *str, int prec);

char *rstripzero(char *str);

char *strmpd(char *buf, size_t buf_size, const mpd_t *value);
json_t *json_string_mpd(const mpd_t *value);
int json_object_set_new_mpd(json_t *obj, const char *key, const mpd_t *value);
int json_array_append_new_mpd(json_t *obj, const mpd_t *value);

# endif

