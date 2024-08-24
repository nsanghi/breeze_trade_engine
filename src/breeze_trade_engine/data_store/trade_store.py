# stub class to think of rest of logic
import logging


# TODO: Plaeeholder - need to check and update the methods
class TradeMaster:
    def __init__(self, file_path, file_name):
        self.orders = []
        self.fills = []
        self.logger = logging.getLogger(__name__)
        self.file = f"{file_path}/{file_name}"

    def add_order(self, order):
        self.orders.append(order)
        self.logger.info(f"Added order: {order}")

    def add_fill(self, fill):
        self.fills.append(fill)
        self.logger.info(f"Added fill: {fill}")

    def modify_order(self, order_id, changes):
        for i, o in enumerate(self.orders):
            if o.order_id == order_id:
                self.orders[i] = {**self.orders[i], **changes}
                self.logger.info(f"Modified order: {order_id} with {changes}")
                break

    def get_orders(self):
        return self.orders

    def get_fills(self):
        return self.fills

    def clear_orders(self):
        self.orders = []
        self.logger.info("Orders cleared.")

    def clear_fills(self):
        self.fills = []
        self.logger.info("Fills cleared.")

    def clear_all(self):
        self.clear_orders()
        self.clear_fills()
        self.logger.info("All orders and fills cleared.")

    def __str__(self):
        return f"Orders: {self.orders}, Fills: {self.fills}"

    def __repr__(self):
        return self.__str__()

    def __len__(self):
        return len(self.orders) + len(self.fills)

    def get_strike_prices(self):
        return set([order.strike_price for order in self.orders])

    def save(self):
        with open(self.ffile_name, "w") as f:
            f.write({"orders": self.orders, "fills": self.fills})

    def load(self):
        with open(self.file_name, "r") as f:
            data = f.read()
            self.orders = data["orders"]
            self.fills = data["fills"]

    def load_open(self):
        with open(self.file_name, "r") as f:
            data = f.read()
            self.orders = data["orders"]
            self.fills = data["fills"]
            self.logger.info("Loaded open orders and fills.")
