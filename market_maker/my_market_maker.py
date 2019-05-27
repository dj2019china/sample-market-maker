from market_maker.market_maker import *
from datetime import datetime
from market_maker.utils import log, constants, errors, math
from math import sqrt
import subprocess
import pickle

logger = log.setup_custom_logger('root')

class MyOrderManager(OrderManager):
    def __init__(self):
        # sys.stdout.write("MyOrderManager is born!\n")
        self.ema_t  = settings.EMA_TIME
        # self.ret_t = 5 # 5 seems too long, chang it to 3
        self.ret_t  = settings.RET_TIME # this is still hard coding
        self.size_mul = settings.SIZE_MUL # start with 0.15 size
        self.lambda_  = 2.0 ** (-1 / self.ema_t)
        self.prev_px, self.cur_px = 0, 0
        try:
            with open('prev_cur_px.pickle', 'rb') as dump:
                self.prev_px, self.cur_px = pickle.load(dump)
        except:
            print('prev_cur_px.pickle don\'t exist!')
        # neg_ret and pos_ret is the past 5 min ret mean
        self.neg_ret, self.pos_ret = 0.003, 0.003
        try:
            with open('neg_pos_ret.pickle', 'rb') as dump:
                self.neg_ret, self.pos_ret = pickle.load(dump)
        except:
            print('neg_pos_ret.pickle don\'t exist!')
        self.prev_time, self.cur_time = datetime.now(), datetime.now()
        OrderManager.__init__(self)

    def update_ret(self):
        self.cur_px, self.prev_px = self.exchange.get_ticker()['last'], self.cur_px
        with open('prev_cur_px.pickle', 'wb') as dump:
            pickle.dump((self.prev_px, self.cur_px), dump)
        self.prev_time, self.cur_time = self.cur_time, datetime.now()
        num_t = self.ret_t * 60 / (self.cur_time - self.prev_time).total_seconds()
        ret  = self.cur_px / self.prev_px - 1 if self.prev_px else 0
        ret *= num_t
        if ret > 0:
            self.pos_ret = self.pos_ret * self.lambda_ + ret * (1 - self.lambda_)
        elif ret < 0:
            self.neg_ret = self.neg_ret * self.lambda_ - ret * (1 - self.lambda_)
        else:
            self.pos_ret = self.pos_ret * sqrt(self.lambda_)
            self.neg_ret = self.neg_ret * sqrt(self.lambda_)
        self.pos_ret = max(settings.MINVOL, min(settings.MAXVOL, self.pos_ret))
        self.neg_ret = max(settings.MINVOL, min(settings.MAXVOL, self.neg_ret))
        # we need to dump
        with open('neg_pos_ret.pickle', 'wb') as dump:
            pickle.dump((self.neg_ret, self.pos_ret), dump)
        print('return: {}, neg_ret: {}, pos_ret: {}'.format(ret, self.pos_ret, self.neg_ret))

    def place_orders(self):
        """Create order items for use in convergence."""
        fund = self.exchange.get_margin()
        ticker = self.exchange.get_ticker()
        instrument = self.exchange.get_instrument()
        position = self.exchange.get_delta()
        tickSize = instrument['tickSize']
        # inorder to determine spread, one need to update return first
        self.update_ret()
        px_list  = settings.PX_LIST
        qty_list = settings.QTY_LIST
        buy_px  = [math.toNearest(ticker["buy"]  * (1 - i * self.neg_ret), tickSize) for i in px_list]
        sell_px = [math.toNearest(ticker["sell"] * (1 + i * self.pos_ret), tickSize) for i in px_list]
        # total_available = 0
        if self.exchange.dry_run:
            total_available = self.size_mul * fund['availableFunds'] / (self.instrument['highPrice'] * self.instrument['initMargin'])
        else:
            total_available = self.size_mul * fund['availableMargin'] / (self.instrument['highPrice'] * self.instrument['initMargin'])
        total_long  = max(0, total_available - 1 * position)
        total_short = max(0, total_available + 1 * position)
        # qty_unit = math.toNearest(self.size_mul * total_available / sum(qty_list), 50)
        long_unit  = math.toNearest(total_long / sum(qty_list), 50)
        short_unit = math.toNearest(total_short / sum(qty_list), 50)
        buy_qty  = [i*long_unit for i in qty_list]
        sell_qty = [i*short_unit for i in qty_list]
        buy_orders  = [{'price': price, 'orderQty': quantity, 'side': "Buy"} for price, quantity in zip(buy_px, buy_qty)]
        sell_orders = [{'price': price, 'orderQty': quantity, 'side': "Sell"} for price, quantity in zip(sell_px, sell_qty)]
        return self.converge_orders(buy_orders, sell_orders)

    def run_loop(self):
        cnt, reset_num = 0, settings.RESET_TIME // settings.LOOP_INTERVAL
        while True:
            sys.stdout.write("-----\n")
            sys.stdout.flush()
            self.check_file_change()
            sleep(settings.LOOP_INTERVAL)
            # This will restart on very short downtime, but if it's longer,
            # the MM will crash entirely as it is unable to connect to the WS on boot.
            if not self.check_connection():
                logger.error("Realtime data connection unexpectedly cself.lambbalosed, restarting.")
                self.restart()
            self.sanity_check()  # Ensures health of mm - several cut-out points here
            self.print_status()  # Print skew, delta, etc
            self.place_orders()  # Creates desired orders and converges to existing orders
            cnt += 1
            if cnt%reset_num == 0:
                self.reset()
def run():
    # logger.info('BitMEX Market Maker Version: %s\n' % constants.VERSION)
    myom = MyOrderManager()
    # Try/except just keeps ctrl-c from printing an ugly stacktrace
    try:
        myom.run_loop()
    except(KeyboardInterrupt):
        sys.exit()
    except(SystemExit):
        sleep(5)
        subprocess.Popen(['/home/chenduo/github/dj/dev_market_maker/mymarketmaker'])
