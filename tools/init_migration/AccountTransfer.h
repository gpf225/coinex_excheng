#ifndef ACCOUNTTRANSFER_H_INCLUDED
#define ACCOUNTTRANSFER_H_INCLUDED

#include "Transfer.h"
#include "Account.h"
#include "Statistic.h"
#include "AccountWriter.h"

class AccountTransfer : public Transfer {
public:
    AccountTransfer(Context &ctx):Transfer(ctx,"account-transfer") {
    }
private:
    int init();
    int handle();
    int save();
    int convert(src_ns::Account &src_acccount);

    vector<shared_ptr<balance_t>> balances_;
    shared_ptr<BalanceWriter> balance_writer_;
};


#endif // ACCOUNTTRANSFER_H_INCLUDED
