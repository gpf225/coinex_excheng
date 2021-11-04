

#include "DealRedisWriter.h"
#include "Utils.h"

extern "C" {
# include "ut_json_rpc.h"
}

char *DealRedisWriter::to_str(deal_t *data) {
    json_t *deal = json_object();
    json_object_set_new(deal, "id", json_integer(data->deal_id));
    json_object_set_new(deal, "time", json_real(data->time));
    json_object_set_new(deal, "ask_user_id", json_integer(data->deal_user_id));
    json_object_set_new(deal, "bid_user_id", json_integer(data->user_id));
    json_object_set_new_mpd(deal, "price", data->price);
    json_object_set_new_mpd(deal, "amount", data->amount);
    if (data->role == 2) { /// is_taker
        json_object_set_new(deal, "type", json_string("buy"));
    } else {
        json_object_set_new(deal, "type", json_string("sell"));
    }

    char *str = json_dumps(deal, 0);
    json_decref(deal);
    return str;
}


int DealRedisWriter::i_save(deal_t *data, const char *str,uint16_t *num) {
    string keys[] = {
        Utils::format_string("k:%s:deals",data->market),
        Utils::format_string("k:%s:real_deals",data->market)
    };
    for (auto key : keys) {
        if (redisAppendCommand(redis_ctx_, "LPUSH %s %s", key.c_str(), str)!=REDIS_OK)
            return -1;
    }

    *num = 2;

    return 0;
}


