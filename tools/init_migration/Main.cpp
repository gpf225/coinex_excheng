#include "Runner.h"

int main() {
    Runner runner;
    int ret = runner.init();
    if (ret) {
        error(EXIT_FAILURE, errno, "init fail: %d", ret);
    }
    runner.run();

    return 0;
}

