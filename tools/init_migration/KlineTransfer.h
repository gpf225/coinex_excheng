#ifndef KLINETRANSFER_H_INCLUDED
#define KLINETRANSFER_H_INCLUDED

#include "Transfer.h"
#include "Kline.h"
#include "FieldSelector.h"
#include "KlineWriter.h"
#include "KlineRedisWriter.h"

class KlineTransfer : public Transfer {
public:
    KlineTransfer(Context &ctx);


private:
    int init();
    int handle();
    int save();
    int save_to_redis();

    template<class T> int load_data(const string &sql,vector<shared_ptr<T>> &result);
    int handle(MYSQL_ROW &row,FieldSelector &selector,shared_ptr<src_ns::Kline> &kline);

    int convert(src_ns::Kline &src_kline);
    void sort_kline(kline_info_t *kline_info);
    int save(uint32_t segment_id,vector<kline_info_t*> &v);

    vector<shared_ptr<kline_info_t>> kline_infos_;
    shared_ptr<KlineWriter> writer_;
    map<uint32_t,vector<kline_info_t*>> kline_info_segments_; /// key is YYYYMM

    shared_ptr<KlineRedisWriter> redis_writer_;
};

#endif // KLINETRANSFER_H_INCLUDED
