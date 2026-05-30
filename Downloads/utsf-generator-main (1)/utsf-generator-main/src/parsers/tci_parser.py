import pandas as pd
import json
import os

class TCIParser:
    def __init__(self):
        base_dir = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
        
        # Load your data sources
        self.rates_path = os.path.join(base_dir, "data", "tci_rates.json")
        self.pincode_path = os.path.join(base_dir, "data", "pincodes.xlsx")

        with open(self.rates_path, "r") as f:
            self.rate_matrix = json.load(f)

        self.pincode_df = pd.read_excel(self.pincode_path)

    def get_zone(self, pincode):
        row = self.pincode_df[self.pincode_df['Pincode'] == int(pincode)]
        if len(row) == 0:
            return "UNKNOWN"
        return row.iloc[0]['Zone']

    def calculate(self, from_pin, to_pin, weight):
        from_zone = self.get_zone(from_pin)
        to_zone = self.get_zone(to_pin)

        if from_zone not in self.rate_matrix:
            raise Exception(f"Unknown FROM zone: {from_zone}")
        if to_zone not in self.rate_matrix[from_zone]:
            raise Exception(f"Unknown TO zone: {to_zone}")

        rate = self.rate_matrix[from_zone][to_zone]

        base = rate * weight
        fuel = base * 0.30
        gst = (base + fuel) * 0.18

        total = base + fuel + gst

        return {
            "from_zone": from_zone,
            "to_zone": to_zone,
            "rate": rate,
            "base": base,
            "fuel": fuel,
            "gst": gst,
            "total": total
        }