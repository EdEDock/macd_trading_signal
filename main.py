import pandas as pd
import numpy as np

# Class for trading system
class System:
    def __init__(self, name, stop_percent):
        self.name = name
        self.trades = 0
        self.p_and_l = 0
        # position_state 0-No position, 1-Long 2-Short
        self.position_state = 0
        self.entry_date = ""
        self.entry_price = 0
        self.stop_price = 0
        self.stop_percent = stop_percent

    def increase_trades_one(self):
        self.trades += 1

    def add_to_p_and_l(self, profit):
        self.p_and_l = self.p_and_l + profit

    def get_position_state(self):
        return self.position_state

    def set_position_state(self, state):
        self.position_state = state

    def set_entry_date(self, tradedate):
        self.entry_date = tradedate

    def set_entry_price(self, price):
        self.entry_price = price

    def set_stop_price(self, price):
        self.stop_price = price

    def get_stop_percent(self):
        return self.stop_percent

    def close_open_positions(self, exit_date, exit_price, trade_list):

        # Check if ther is an open position
        if self.position_state != 0:

            # Check if long position
            if self.position_state == 1:
                p_and_l = exit_price - self.entry_price

            # Check if short position
            elif self.position_state == 2:
                p_and_l = self.entry_price - exit_price

            self.increase_trades_one()
            self.add_to_p_and_l(p_and_l)
            trade_list.append(tuple([self.name, self.entry_date, "LONG" if self.position_state == 1 else "SHORT",
                                     self.entry_price, exit_date, exit_price, p_and_l]))
            # We no longer have an open position
            self.set_position_state(0)

    def display_results(self):

        print("Trades: {} , P & L: {}".format(self.trades, self.p_and_l))

    def check_stops(self, row, trade_list):

        # Check if a position exists
        if self.position_state != 0:

            if self.position_state == 1:
                # Check if Long Position was Stopped Out
                if row.low_x <= self.stop_price:
                    macd.close_open_positions(row.Index, self.stop_price, trade_list)
                    print("Long Stop: {} @ {}".format(row.Index, self.stop_price))

            if self.position_state == 2:
                # Check if Short Position was Stopped Out
                if row.high_x >= self.stop_price:
                    macd.close_open_positions(row.Index, self.stop_price, trade_list)
                    print("Short Stop: {} @ {}".format(row.Index, self.stop_price))


def get_dataframe(infile):

    # Dataframe is expecting individual trades which contain a date(yyyy-mm-dd hh:mm:ss) and a price
    data_frame = pd.read_csv(infile, header=None, usecols=[1, 3], names=['price', 'tradedate'], index_col=1,
                             parse_dates=True)

    # Get 15 Minute Bars for SMA calculations
    bars_15min = data_frame['price'].resample('15Min').ohlc().ffill()

    # Calculate SMA
    # Set Period in Days
    sma_period = 25
    bars_15min['sma'] = bars_15min['close'].rolling(window=sma_period).mean()

    # Get 5 Minute Bars for MACD calculations
    bars_5min = data_frame['price'].resample('5Min').ohlc().ffill()

    # Set Period in Days
    ema1_period = 12

    # ema1 - Calculation for 1st row is a rolling average
    bars_5min['ema1'] = bars_5min['close'].rolling(window=ema1_period).mean()

    for i in range(len(bars_5min)):

        if i < ema1_period - 1:
            # Not enough data available for calculation use NaN
            bars_5min.ema1.iloc[i] = np.nan
        elif i > 11:
            # Formula for rows after 1st row
            bars_5min.ema1.iloc[i] = (bars_5min.close.iloc[i] * (2 / (ema1_period + 1)) + bars_5min.ema1.iloc[i - 1] *
                                      (1 - (2 / (ema1_period + 1))))

    # Set Period in Days
    ema2_period = 26

    # ema2 - Calculation for 1st row is a rolling average
    bars_5min['ema2'] = bars_5min['close'].rolling(window=ema2_period).mean()

    for i in range(len(bars_5min)):
        if i < ema2_period - 1:
            # Not enough data available for calculation use NaN
            bars_5min.ema2.iloc[i] = np.nan
        elif i > 25:
            # Formula for rows after 1st row
            bars_5min.ema2.iloc[i] = (
                        bars_5min.close.iloc[i] * (2 / (ema2_period + 1)) + bars_5min.ema2.iloc[i - 1] * (
                            1 - (2 / (ema2_period + 1))))

    # macd
    bars_5min['macd'] = bars_5min['ema1'] - bars_5min['ema2']

    # Signal - Calculation for 1st row is a rolling average
    bars_5min['signal'] = bars_5min['macd'].rolling(window=9).mean()

    for i in range(len(bars_5min)):

        if i < ema2_period + 7:
            # Not enough data available for calculation use NaN
            bars_5min.signal.iloc[i] = np.nan
        elif i > ema2_period + 7:
            bars_5min.signal.iloc[i] = (
                        bars_5min.macd.iloc[i] * (2 / (9 + 1)) + bars_5min.signal.iloc[i - 1] * (1 - (2 / (9 + 1))))

    # historgram
    bars_5min['histogram'] = bars_5min['macd'] - bars_5min['signal']
    # previous histogram
    bars_5min['prev_histogram'] = bars_5min['histogram'].shift(1)

    # Get N+1 High,Low,Open  N+2 Open for execution prices
    bars_5min['next_low'] = bars_5min['low'].shift(-1)
    bars_5min['next_high'] = bars_5min['high'].shift(-1)
    bars_5min['next_open'] = bars_5min['open'].shift(-1)
    bars_5min['open_nplus2'] = bars_5min['open'].shift(-2)

    # Merge SMA dataframe with MACD dataframe for easier iteration
    merged_dataset = pd.merge(bars_5min, bars_15min, left_index=True, right_index=True, how='outer')

    # Forward Fill 15 minute SMA bars, Close price values so 5 minute bars have data
    merged_dataset['sma'] = merged_dataset['sma'].ffill()
    merged_dataset['close_y'] = merged_dataset['close_y'].ffill()

    # Remove unnecessary columns
    merged_dataset = merged_dataset.drop(['open_y', 'high_y', 'low_y'], axis=1)

    # Write Values for review
    merged_dataset.to_csv('data.csv')

    merged_dataset = merged_dataset.dropna()

    merged_dataset.to_csv('data2.csv')

    # Only return rows where we have a values for all indicators
    return merged_dataset.dropna()

if __name__ == '__main__':

    df = get_dataframe("XXBTZUSD_2017ON.csv")

    trade_list = []

    # Create system object with name and stop %
    macd = System("macd", .10)

    for row in df.itertuples():

        # Check if any stops where hit
        macd.check_stops(row, trade_list)

        # Check for Long Entry Signals
        if ((row.prev_histogram < 0) and (row.histogram > 0)) and row.close_y > row.sma and macd.get_position_state() == 0:

            # Determine if limit of close(n) would execute N+1. Use Low for a buy!
            # Example Buy Limit @ 10 will execute with next bar low <= 10
            if row.close_x >= row.next_low:
                print("Long Entry: {} @ {}".format(row.Index, row.close_x))

                # Set Position State to 1 (Open Long Position)
                macd.set_position_state(1)
                macd.set_entry_date(row.Index)
                macd.set_entry_price(row.close_x)
                macd.set_stop_price(row.close_x - (row.close_x * macd.get_stop_percent()))

        # Check for Long Exit Signals
        elif row.prev_histogram > row.histogram and macd.get_position_state() == 1:

            # Determine if limit of close(n) would execute N+1. Use High for a sell!
            # Example Sell Limit @ 10 will execute with next bar high >= 10
            if row.close_x <= row.next_high:
                exit_price = row.close_x
                print("Long Exit: {} @ {}".format(row.Index, row.close_x))

            else:
                exit_price = row.open_nplus2
                # Must Exit N+2 at the open
                print("Long Exit: {} @ {}".format(row.Index, row.open_nplus2))

            macd.close_open_positions(row.Index, exit_price, trade_list)

        # Check for Short Entry Signals
        elif ((row.prev_histogram > 0) and (row.histogram < 0)) and row.close_y < row.sma and macd.get_position_state() == 0:

            # Determine if limit of close(n) would execute N+1. Use High for a sell!
            # Example Sell Limit @ 10 will execute with next bar high >= 10
            if row.close_x <= row.next_high:
                print("Short Entry: {} @ {}".format(row.Index, row.close_x))

                # Set Position State to 2 (Open Short Position)
                macd.set_position_state(2)
                macd.set_entry_date(row.Index)
                macd.set_entry_price(row.close_x)
                macd.set_stop_price(row.close_x + (row.close_x * macd.get_stop_percent()))

        # Check for Short Exit Signals
        elif row.prev_histogram < row.histogram and macd.get_position_state() == 2:

            # Determine if limit of close(n) would execute N+1. Use Low for a buy!
            # Example Buy Limit @ 10 will execute with next bar low <= 10
            if row.close_x >= row.next_low:
                exit_price = row.close_x
                print("Short Exit: {} @ {}".format(row.Index, row.close_x))
            else:
                # Must Exit N+2 at the open
                exit_price = row.open_nplus2
                print("Short Exit: {} @ {}".format(row.Index, row.open_nplus2))

            macd.close_open_positions(row.Index, exit_price, trade_list)

    # Close out any open positions when data has been exhausted. Use next open for execution price
    macd.close_open_positions(row.Index, row.next_open, trade_list)

    macd.display_results()

    # Write Trades to trade.csv
    trade_df = pd.DataFrame(trade_list)
    trade_df.to_csv('trades.csv')