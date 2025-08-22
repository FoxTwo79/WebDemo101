import yfinance as yf
import pandas as pd
from datetime import datetime, time
import json
import os

# Stock lists
PENNY = [
    "HPEKW","THAR","ALUDW","NBY","SVREW","ATCL","RCKTW","CPOD","RCXT","GES","GLIN",
    "GLE","CCGWW","JUNS","CGTX","RAVE","FONR","PRLD","MSC","ZVYT","STEM","MBT","TLST"
]

ABOVE5 = [
    "SISI","RADX","SBEV","CLNN","MSC","AHL","INTEG","MIMI","SUPX","MEXS","SRRK","NESR","RMSG",
    "HTOOR","MDRR","NEON","KELYB","TVRD","IROH","DK","ATGL","GES"
]

ALL_STOCKS = PENNY + ABOVE5
INVESTMENT_PER_STOCK = 100.0
TRADE_FILE = "paper_trades.json"

class PaperTradingBot:
    def __init__(self):
        self.positions = {}
        self.load_positions()
    
    def load_positions(self):
        """Load existing positions from file"""
        if os.path.exists(TRADE_FILE):
            with open(TRADE_FILE, 'r') as f:
                self.positions = json.load(f)
    
    def save_positions(self):
        """Save positions to file"""
        with open(TRADE_FILE, 'w') as f:
            json.dump(self.positions, f, indent=2, default=str)
    
    def get_current_price(self, symbol):
        """Get current stock price"""
        try:
            ticker = yf.Ticker(symbol)
            data = ticker.history(period="1d")
            if not data.empty:
                return data['Close'].iloc[-1]
            else:
                # Try premarket data
                data = ticker.history(period="1d", prepost=True)
                if not data.empty:
                    return data['Close'].iloc[-1]
        except Exception as e:
            print(f"Error getting price for {symbol}: {e}")
        return None
    
    def buy_stock(self, symbol, price):
        """Execute buy order"""
        shares = INVESTMENT_PER_STOCK / price
        
        self.positions[symbol] = {
            'symbol': symbol,
            'shares': shares,
            'buy_price': price,
            'buy_time': datetime.now().isoformat(),
            'investment': INVESTMENT_PER_STOCK,
            'status': 'open'
        }
        
        print(f"BOUGHT: {symbol} - {shares:.4f} shares @ ${price:.4f} = ${INVESTMENT_PER_STOCK}")
    
    def sell_stock(self, symbol, price):
        """Execute sell order"""
        if symbol not in self.positions:
            print(f"No position found for {symbol}")
            return
        
        position = self.positions[symbol]
        shares = position['shares']
        buy_price = position['buy_price']
        
        sell_value = shares * price
        pnl = sell_value - INVESTMENT_PER_STOCK
        pnl_percent = (pnl / INVESTMENT_PER_STOCK) * 100
        
        position.update({
            'sell_price': price,
            'sell_time': datetime.now().isoformat(),
            'sell_value': sell_value,
            'pnl': pnl,
            'pnl_percent': pnl_percent,
            'status': 'closed'
        })
        
        print(f"SOLD: {symbol} - {shares:.4f} shares @ ${price:.4f} = ${sell_value:.2f}")
        print(f"P&L: ${pnl:.2f} ({pnl_percent:.2f}%)")
    
    def buy_all_stocks(self):
        """Buy all stocks in the list"""
        print(f"\n{'='*60}")
        print(f"BUYING ALL STOCKS - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'='*60}")
        
        for symbol in ALL_STOCKS:
            if symbol in self.positions and self.positions[symbol]['status'] == 'open':
                print(f"SKIP: {symbol} - Already have open position")
                continue
                
            price = self.get_current_price(symbol)
            if price:
                self.buy_stock(symbol, price)
            else:
                print(f"SKIP: {symbol} - Could not get price")
        
        self.save_positions()
        print(f"\nBuy orders completed. Total investment: ${len([p for p in self.positions.values() if p['status'] == 'open']) * INVESTMENT_PER_STOCK}")
    
    def sell_all_stocks(self):
        """Sell all open positions"""
        print(f"\n{'='*60}")
        print(f"SELLING ALL STOCKS - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'='*60}")
        
        open_positions = [p for p in self.positions.values() if p['status'] == 'open']
        
        if not open_positions:
            print("No open positions to sell")
            return
        
        for symbol in [p['symbol'] for p in open_positions]:
            price = self.get_current_price(symbol)
            if price:
                self.sell_stock(symbol, price)
            else:
                print(f"SKIP: {symbol} - Could not get current price")
        
        self.save_positions()
        self.show_summary()
    
    def show_portfolio(self):
        """Show current portfolio status"""
        print(f"\n{'='*80}")
        print(f"PORTFOLIO STATUS - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'='*80}")
        
        if not self.positions:
            print("No positions")
            return
        
        open_positions = []
        closed_positions = []
        
        for symbol, pos in self.positions.items():
            if pos['status'] == 'open':
                current_price = self.get_current_price(symbol)
                if current_price:
                    current_value = pos['shares'] * current_price
                    unrealized_pnl = current_value - INVESTMENT_PER_STOCK
                    unrealized_pnl_pct = (unrealized_pnl / INVESTMENT_PER_STOCK) * 100
                    
                    open_positions.append({
                        'symbol': symbol,
                        'shares': pos['shares'],
                        'buy_price': pos['buy_price'],
                        'current_price': current_price,
                        'investment': INVESTMENT_PER_STOCK,
                        'current_value': current_value,
                        'unrealized_pnl': unrealized_pnl,
                        'unrealized_pnl_pct': unrealized_pnl_pct
                    })
            else:
                closed_positions.append(pos)
        
        # Show open positions
        if open_positions:
            print("\nOPEN POSITIONS:")
            print(f"{'Symbol':<8} {'Shares':<10} {'Buy Price':<12} {'Current':<12} {'Value':<12} {'P&L':<12} {'P&L%':<8}")
            print("-" * 80)
            
            total_investment = 0
            total_current_value = 0
            
            for pos in open_positions:
                total_investment += pos['investment']
                total_current_value += pos['current_value']
                
                print(f"{pos['symbol']:<8} {pos['shares']:<10.4f} ${pos['buy_price']:<11.4f} "
                      f"${pos['current_price']:<11.4f} ${pos['current_value']:<11.2f} "
                      f"${pos['unrealized_pnl']:<11.2f} {pos['unrealized_pnl_pct']:<7.2f}%")
            
            total_unrealized_pnl = total_current_value - total_investment
            total_unrealized_pnl_pct = (total_unrealized_pnl / total_investment) * 100 if total_investment > 0 else 0
            
            print("-" * 80)
            print(f"{'TOTAL':<8} {'':<10} {'':<12} {'':<12} ${total_current_value:<11.2f} "
                  f"${total_unrealized_pnl:<11.2f} {total_unrealized_pnl_pct:<7.2f}%")
        
        # Show closed positions
        if closed_positions:
            print("\nCLOSED POSITIONS:")
            print(f"{'Symbol':<8} {'Shares':<10} {'Buy Price':<12} {'Sell Price':<12} {'P&L':<12} {'P&L%':<8}")
            print("-" * 80)
            
            total_realized_pnl = 0
            
            for pos in closed_positions:
                total_realized_pnl += pos['pnl']
                print(f"{pos['symbol']:<8} {pos['shares']:<10.4f} ${pos['buy_price']:<11.4f} "
                      f"${pos['sell_price']:<11.4f} ${pos['pnl']:<11.2f} {pos['pnl_percent']:<7.2f}%")
            
            print("-" * 80)
            print(f"TOTAL REALIZED P&L: ${total_realized_pnl:.2f}")
    
    def show_summary(self):
        """Show trading summary"""
        if not self.positions:
            return
        
        closed_positions = [p for p in self.positions.values() if p['status'] == 'closed']
        
        if not closed_positions:
            return
        
        total_investment = len(closed_positions) * INVESTMENT_PER_STOCK
        total_return = sum(p['sell_value'] for p in closed_positions)
        total_pnl = total_return - total_investment
        total_pnl_pct = (total_pnl / total_investment) * 100
        
        winners = [p for p in closed_positions if p['pnl'] > 0]
        losers = [p for p in closed_positions if p['pnl'] < 0]
        
        print(f"\n{'='*60}")
        print(f"TRADING SUMMARY")
        print(f"{'='*60}")
        print(f"Total Trades: {len(closed_positions)}")
        print(f"Winners: {len(winners)} ({len(winners)/len(closed_positions)*100:.1f}%)")
        print(f"Losers: {len(losers)} ({len(losers)/len(closed_positions)*100:.1f}%)")
        print(f"Total Investment: ${total_investment:.2f}")
        print(f"Total Return: ${total_return:.2f}")
        print(f"Total P&L: ${total_pnl:.2f} ({total_pnl_pct:.2f}%)")
        
        if winners:
            avg_winner = sum(p['pnl'] for p in winners) / len(winners)
            print(f"Average Winner: ${avg_winner:.2f}")
        
        if losers:
            avg_loser = sum(p['pnl'] for p in losers) / len(losers)
            print(f"Average Loser: ${avg_loser:.2f}")

def main():
    bot = PaperTradingBot()
    
    while True:
        print(f"\n{'='*60}")
        print("PAPER TRADING BOT")
        print(f"{'='*60}")
        print("1. Buy all stocks")
        print("2. Sell all stocks")
        print("3. Show portfolio")
        print("4. Show summary")
        print("5. Exit")
        
        choice = input("\nEnter your choice (1-5): ").strip()
        
        if choice == '1':
            bot.buy_all_stocks()
        elif choice == '2':
            bot.sell_all_stocks()
        elif choice == '3':
            bot.show_portfolio()
        elif choice == '4':
            bot.show_summary()
        elif choice == '5':
            print("Exiting...")
            break
        else:
            print("Invalid choice. Please try again.")

if __name__ == "__main__":
    main()