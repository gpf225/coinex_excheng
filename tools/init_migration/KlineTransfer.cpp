
#include "KlineTransfer.h"
#include "Utils.h"

KlineTransfer::KlineTransfer(Context &ctx):Transfer(ctx,"kline-transfer") {
}

int KlineTransfer::init() {
    writer_ = make_shared<KlineWriter>(ctx_);
    redis_writer_ = make_shared<KlineRedisWriter>(ctx_);
    return 0;
}


int KlineTransfer::handle(MYSQL_ROW &row,FieldSelector &selector,shared_ptr<src_ns::Kline> &kline) {
    kline->appl_id = strtoul(row[selector["appl_id"]],nullptr,0);
    kline->contract_id = strtoul(row[selector["contract_id"]],nullptr,0);
    kline->range = strtoull(row[selector["range"]],nullptr,0);
    kline->time = strtoull(row[selector["time"]],nullptr,0);
    kline->open_price = row[selector["open_price"]];
    kline->close_price = row[selector["close_price"]];
    kline->high_price = row[selector["high_price"]];
    kline->low_price = row[selector["low_price"]];
    kline->volume = row[selector["volume"]];

    return 0;
}

template<class T> int KlineTransfer::load_data(const string &sql,vector<shared_ptr<T>> &result) {
    int ret = mysql_real_query(ctx_.src_db_conn_, sql.c_str(), sql.length());
    if (ret != 0) {
        log_error("exec sql: %s fail: %d %s", sql.c_str(), mysql_errno(ctx_.src_db_conn_), mysql_error(ctx_.src_db_conn_));
        return -1;
    }

    MYSQL_RES *res = mysql_store_result(ctx_.src_db_conn_);
    stat_->total_ += mysql_num_rows(res);
    FieldSelector selector(res);

    MYSQL_ROW row;
    while ((row = mysql_fetch_row(res)))  {
        shared_ptr<T> d(new T());
        handle(row,selector,d);
        result.push_back(d);
    }

    return 0;
}

int KlineTransfer::handle() {
    string sql = Utils::format_string("select * from core_candlestick where `range` in(60000000,1800000000,86400000000)");
    using Kline=src_ns::Kline;
    vector<shared_ptr<Kline>> klines;

    int ret = load_data(sql,klines);
    if (ret)
        return -1;
    for (auto kline : klines) {
        int ret = convert(*kline.get());
        if (ret) {
            stat_->fail_++;
            log_error("convert kline contract_id:%d,range:%" PRIu64 ",time:%" PRIu64,
                      kline->contract_id,kline->range,kline->time);
        }
        else {
            stat_->success_++;
        }
    }

    if (save())
        return -1;

    if (save_to_redis())
        return -1;

    return 0;
}


void KlineTransfer::sort_kline(kline_info_t *kline_info) {
    time_t t = kline_info->timestamp;
    struct tm *tm = gmtime(&t);
    uint32_t key = (tm->tm_year+1900)*100 + tm->tm_mon+1;


    auto it = kline_info_segments_.find(key);
    if (it==kline_info_segments_.end()) {
        vector<kline_info_t*> v;
        v.push_back(kline_info);
        kline_info_segments_[key] = v;
    }
    else {
        auto &v = it->second;
        v.push_back(kline_info);
    }
}

int KlineTransfer::save(uint32_t segment_id,vector<kline_info_t*> &kline_infos) {
    writer_->segment_id_ = segment_id;
    int ret = writer_->generate_table();
    if (ret)
        return -1;

    CLargeStringArray data;
    for (auto kline_info : kline_infos) {
        string value = writer_->generate_value_list(*kline_info);
        data.Add(Utils::dup_string(value));
    }
    ret = writer_->batch_insert(data,ctx_.settings_.row_limit);
    if (ret)
        return -1;

    return 0;
}

int KlineTransfer::save() {
    for ( auto segment : kline_info_segments_) {
        if (save(segment.first,segment.second))
                return -1;
    }
    return 0;
}

int map_kline_type(uint64_t range) {
    switch(range) {
    case 60000000: return INTERVAL_MIN;
    case 1800000000: return INTERVAL_HOUR;
    case 86400000000: return INTERVAL_DAY;
    }
    assert(false);
    return -1;
}

int KlineTransfer::convert(src_ns::Kline &src_kline) {
    kline_info_t *kline_info = new kline_info_t();
    const Market *market = ctx_.get_market(src_kline.contract_id);
    if (market==nullptr)
        return -1;
    kline_info->market = strdup(market->name_.c_str());
    kline_info->type = map_kline_type(src_kline.range);
    kline_info->timestamp = src_kline.time/1000000;

    kline_info->open = decimal(src_kline.open_price.c_str(),0);
    kline_info->close = decimal(src_kline.close_price.c_str(),0);
    kline_info->high = decimal(src_kline.high_price.c_str(),0);
    kline_info->low = decimal(src_kline.low_price.c_str(),0);
    kline_info->volume = decimal(src_kline.volume.c_str(),0);

    kline_infos_.push_back(shared_ptr<kline_info_t>(kline_info));

    sort_kline(kline_info);

    return 0;
 }


int KlineTransfer::save_to_redis() {
    time_t now = time(NULL);
    for (auto kline_info : kline_infos_) {
        kline_info_t *kinfo = kline_info.get();
        time_t start;
        switch(kinfo->type) {
            case  INTERVAL_MIN:
               start = now / 60 * 60 - settings.min_max * 60; break;
            case INTERVAL_HOUR:
                start =  now / 3600 * 3600 - settings.hour_max * 3600;break;
            case INTERVAL_DAY:
                start = 0;break;
        }
//    if (start&&kinfo->timestamp<start)
//        return 0;
        redis_writer_->save(kinfo);
    }
    return 0;
}
