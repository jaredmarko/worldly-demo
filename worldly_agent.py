import os
import sqlite3
from typing import List, Dict, Any
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv
import redis
import json
import plotly.express as px
from datetime import datetime
import pandas as pd
import requests
from fuzzywuzzy import fuzz

# Load environment variables
load_dotenv()
WEATHER_API_KEY = os.getenv("WEATHER_API_KEY")
if not WEATHER_API_KEY:
    raise ValueError("Missing WEATHER_API_KEY in environment variables.")

# Redis setup
redis_client = redis.StrictRedis(host="localhost", port=6379, db=0, decode_responses=True)

# Step 1: Initialize Database with Expanded Real-World Data
def initialize_sustainability_db(db_path: str = "worldly_risk.db") -> None:
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute("DROP TABLE IF EXISTS suppliers;")
    cursor.execute("DROP TABLE IF EXISTS products;")
    cursor.execute("DROP TABLE IF EXISTS supplier_history;")

    cursor.execute('''
        CREATE TABLE suppliers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            location TEXT,
            latitude REAL,
            longitude REAL,
            carbon_footprint REAL,
            water_usage REAL,
            compliance_score REAL CHECK(compliance_score BETWEEN 0 AND 1)
        )
    ''')

    cursor.execute('''
        CREATE TABLE products (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            supplier_id INTEGER,
            production_date TEXT,
            carbon_per_unit REAL,
            water_per_unit REAL,
            material TEXT,
            FOREIGN KEY (supplier_id) REFERENCES suppliers(id)
        )
    ''')

    cursor.execute('''
        CREATE TABLE supplier_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            supplier_id INTEGER,
            year TEXT,
            carbon_footprint REAL,
            water_usage REAL,
            compliance_score REAL,
            FOREIGN KEY (supplier_id) REFERENCES suppliers(id)
        )
    ''')

    # Expanded real-world supplier data
    suppliers_data = [
        ("Shahjalal Textile Mills", "Dhaka, Bangladesh", 23.8103, 90.4125, 1450.0, 18000.0, 0.82),
        ("Marzotto Group", "Vicenza, Italy", 45.5495, 11.5475, 950.0, 11000.0, 0.91),
        ("Patagonia Suppliers", "Ventura, USA", 34.2805, -119.2945, 700.0, 8500.0, 0.96),
        ("Arvind Limited", "Ahmedabad, India", 23.0225, 72.5714, 1300.0, 16000.0, 0.87),
        ("Crystal Group", "Hong Kong, China", 22.3193, 114.1694, 1100.0, 14000.0, 0.89),
        ("Esquel Group", "Guangzhou, China", 23.1291, 113.2644, 900.0, 12000.0, 0.93),
        ("Nishat Mills", "Lahore, Pakistan", 31.5204, 74.3587, 1200.0, 15500.0, 0.84),
        ("Vardhman Textiles", "Ludhiana, India", 30.9010, 75.8573, 1050.0, 13000.0, 0.90),
    ]

    # Expanded product data with material
    products_data = [
        ("Organic Cotton Shirt", 1, "2025-03-20", 0.45, 18.5, "Organic Cotton"),
        ("Wool Sweater", 2, "2025-03-22", 0.35, 12.0, "Wool"),
        ("Recycled Poly Jacket", 3, "2025-03-24", 0.65, 25.0, "Recycled Polyester"),
        ("Denim Jeans", 4, "2025-03-21", 0.55, 22.0, "Denim"),
        ("Polyester Tee", 5, "2025-03-23", 0.40, 15.0, "Polyester"),
        ("Linen Shirt", 6, "2025-03-22", 0.30, 10.0, "Linen"),
        ("Cotton Polo", 7, "2025-03-20", 0.50, 20.0, "Cotton"),
        ("Viscose Dress", 8, "2025-03-23", 0.38, 14.0, "Viscose"),
    ]

    # Expanded historical data (2021-2024)
    supplier_history_data = [
        (1, "2021", 1700.0, 21000.0, 0.78), (1, "2022", 1600.0, 20000.0, 0.80), (1, "2023", 1550.0, 19000.0, 0.81), (1, "2024", 1500.0, 18500.0, 0.82),
        (2, "2021", 1100.0, 13000.0, 0.88), (2, "2022", 1050.0, 12500.0, 0.89), (2, "2023", 1000.0, 12000.0, 0.90), (2, "2024", 975.0, 11500.0, 0.91),
        (3, "2021", 900.0, 10000.0, 0.93), (3, "2022", 850.0, 9500.0, 0.94), (3, "2023", 800.0, 9000.0, 0.95), (3, "2024", 750.0, 8750.0, 0.96),
        (4, "2021", 1500.0, 18000.0, 0.83), (4, "2022", 1450.0, 17500.0, 0.84), (4, "2023", 1400.0, 17000.0, 0.85), (4, "2024", 1350.0, 16500.0, 0.86),
        (5, "2021", 1300.0, 16000.0, 0.86), (5, "2022", 1250.0, 15500.0, 0.87), (5, "2023", 1200.0, 15000.0, 0.88), (5, "2024", 1150.0, 14500.0, 0.89),
        (6, "2021", 1000.0, 14000.0, 0.90), (6, "2022", 975.0, 13500.0, 0.91), (6, "2023", 950.0, 13000.0, 0.92), (6, "2024", 925.0, 12500.0, 0.93),
        (7, "2021", 1400.0, 17000.0, 0.81), (7, "2022", 1350.0, 16500.0, 0.82), (7, "2023", 1300.0, 16000.0, 0.83), (7, "2024", 1250.0, 15750.0, 0.84),
        (8, "2021", 1150.0, 14500.0, 0.87), (8, "2022", 1100.0, 14000.0, 0.88), (8, "2023", 1075.0, 13500.0, 0.89), (8, "2024", 1050.0, 13250.0, 0.90),
    ]

    cursor.executemany("INSERT INTO suppliers (name, location, latitude, longitude, carbon_footprint, water_usage, compliance_score) VALUES (?, ?, ?, ?, ?, ?, ?)", suppliers_data)
    cursor.executemany("INSERT INTO products (name, supplier_id, production_date, carbon_per_unit, water_per_unit, material) VALUES (?, ?, ?, ?, ?, ?)", products_data)
    cursor.executemany("INSERT INTO supplier_history (supplier_id, year, carbon_footprint, water_usage, compliance_score) VALUES (?, ?, ?, ?, ?)", supplier_history_data)
    conn.commit()
    conn.close()

# Step 2: Worldly Sustainability Risk Agent
class WorldlySustainabilityAgent:
    def __init__(self, db_path: str = "worldly_risk.db"):
        self.engine = create_engine(f"sqlite:///{db_path}")
        self.schema = self._get_full_schema()
        self.supplier_names = self._get_supplier_names()

    def _get_full_schema(self) -> str:
        with self.engine.connect() as conn:
            tables = conn.execute(text("SELECT name FROM sqlite_master WHERE type='table';")).fetchall()
            schema = ""
            for table in tables:
                table_name = table[0]
                columns = conn.execute(text(f"PRAGMA table_info({table_name});")).fetchall()
                schema += f"Table: {table_name}\nColumns:\n"
                for col in columns:
                    schema += f"- {col[1]} ({col[2]})\n"
                schema += "\n"
            return schema

    def _get_supplier_names(self) -> List[str]:
        with self.engine.connect() as conn:
            result = conn.execute(text("SELECT name FROM suppliers")).fetchall()
            return [row[0] for row in result]

    def _fetch_weather_data(self, lat: float, lon: float) -> Dict[str, Any]:
        url = f"http://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={WEATHER_API_KEY}&units=metric"
        try:
            response = requests.get(url, timeout=5)
            response.raise_for_status()
            data = response.json()
            return {
                "condition": data["weather"][0]["main"],
                "temp": data["main"]["temp"],
                "wind_speed": data["wind"]["speed"]
            }
        except requests.RequestException as e:
            return {"error": f"Weather API failed: {str(e)}"}

    def _fetch_sustainability_data(self) -> Dict[str, Any]:
        return {
            "Shahjalal Textile Mills": {"emissions_risk": "High", "water_risk": "High"},
            "Marzotto Group": {"emissions_risk": "Moderate", "water_risk": "Low"},
            "Patagonia Suppliers": {"emissions_risk": "Low", "water_risk": "Low"},
            "Arvind Limited": {"emissions_risk": "High", "water_risk": "Moderate"},
            "Crystal Group": {"emissions_risk": "Moderate", "water_risk": "Moderate"},
            "Esquel Group": {"emissions_risk": "Low", "water_risk": "Low"},
            "Nishat Mills": {"emissions_risk": "High", "water_risk": "High"},
            "Vardhman Textiles": {"emissions_risk": "Moderate", "water_risk": "Moderate"}
        }

    def _fetch_external_data(self) -> Dict[str, Any]:
        with self.engine.connect() as conn:
            suppliers = conn.execute(text("SELECT name, latitude, longitude FROM suppliers")).fetchall()
        external_data = {"weather": {}, "sustainability": self._fetch_sustainability_data()}
        for supplier in suppliers:
            name, lat, lon = supplier
            external_data["weather"][name] = self._fetch_weather_data(lat, lon)
        return external_data

    def _calculate_risk_score(self, carbon: float, water: float, compliance: float) -> float:
        norm_carbon = min(carbon / 2000, 1.0)
        norm_water = min(water / 25000, 1.0)
        norm_compliance = 1 - compliance
        return (0.4 * norm_carbon + 0.3 * norm_water + 0.3 * norm_compliance) * 100

    def _fetch_historical_trends(self, supplier_id: int) -> List[Dict[str, Any]]:
        with self.engine.connect() as conn:
            query = text("SELECT year, carbon_footprint, water_usage, compliance_score FROM supplier_history WHERE supplier_id = :sid ORDER BY year")
            result = conn.execute(query, {"sid": supplier_id}).fetchall()
            columns = ["year", "carbon_footprint", "water_usage", "compliance_score"]
            return [dict(zip(columns, row)) for row in result]

    def _predict_future(self, trends: List[Dict[str, Any]], metric: str) -> float:
        if len(trends) < 2:
            return trends[-1][metric] if trends else 0.0
        years = [int(t["year"]) for t in trends]
        values = [t[metric] for t in trends]
        total_change = values[-1] - values[0]
        num_years = years[-1] - years[0]
        annual_change = total_change / num_years
        return values[-1] + annual_change

    def _calculate_trend_percentage(self, trends: List[Dict[str, Any]], metric: str) -> float:
        if len(trends) < 2:
            return 0.0
        start = trends[0][metric]
        end = trends[-1][metric]
        return ((end - start) / start) * 100 if start != 0 else 0.0

    def _cache_result(self, key: str, value: Dict[str, Any], ttl: int = 3600) -> None:
        redis_client.setex(key, ttl, json.dumps(value))

    def _get_cached_result(self, key: str) -> Dict[str, Any] | None:
        cached = redis_client.get(key)
        return json.loads(cached) if cached else None

    def _validate_sql(self, query: str) -> bool:
        try:
            with self.engine.connect() as conn:
                conn.execute(text(query)).fetchall()
            return True
        except SQLAlchemyError:
            return False

    def _fuzzy_match_supplier(self, name: str) -> str:
        best_match = None
        best_score = 0
        for supplier in self.supplier_names:
            score = fuzz.ratio(name.lower(), supplier.lower())
            if score > best_score and score > 80:
                best_score = score
                best_match = supplier
        return best_match

    def generate_sql(self, question: str) -> str:
        question = question.lower()

        # Step 1: Set location based on keywords
        location = None
        if "india" in question:
            location = "India"
        elif "china" in question:
            location = "China"
        elif "usa" in question:
            location = "USA"
        elif "bangladesh" in question:
            location = "Bangladesh"
        elif "pakistan" in question:
            location = "Pakistan"
        elif "italy" in question:
            location = "Italy"

        # Step 2: Set material based on keywords
        material = None
        if "cotton" in question:
            material = "Cotton"
        elif "wool" in question:
            material = "Wool"
        elif "polyester" in question:
            material = "Polyester"
        elif "denim" in question:
            material = "Denim"
        elif "linen" in question:
            material = "Linen"
        elif "viscose" in question:
            material = "Viscose"

        # Step 3: Fuzzy match supplier names
        supplier_name = None
        for name in self.supplier_names:
            if name.lower() in question:
                supplier_name = name
                break
        if not supplier_name:
            for word in question.split():
                match = self._fuzzy_match_supplier(word)
                if match:
                    supplier_name = match
                    break

        # Step 4: Generate SQL based on query type
        # Combined product queries (location + material)
        if "products" in question and location and material:
            if "low carbon footprint" in question:
                return f"SELECT p.name, s.name AS supplier, p.carbon_per_unit FROM products p JOIN suppliers s ON p.supplier_id = s.id WHERE s.location LIKE '%{location}%' AND p.material LIKE '%{material}%' ORDER BY p.carbon_per_unit ASC;"
            if "high water usage" in question or "highest water usage" in question:
                return f"SELECT p.name, s.name AS supplier, p.water_per_unit FROM products p JOIN suppliers s ON p.supplier_id = s.id WHERE s.location LIKE '%{location}%' AND p.material LIKE '%{material}%' ORDER BY p.water_per_unit DESC;"
            return f"SELECT p.name, s.name AS supplier, p.water_per_unit FROM products p JOIN suppliers s ON p.supplier_id = s.id WHERE s.location LIKE '%{location}%' AND p.material LIKE '%{material}%';"

        # Supplier queries with location and metric
        if "suppliers" in question and location:
            if "highest carbon footprint" in question:
                return f"SELECT name, location, carbon_footprint FROM suppliers WHERE location LIKE '%{location}%' ORDER BY carbon_footprint DESC;"
            if "highest water usage" in question:
                return f"SELECT name, location, water_usage FROM suppliers WHERE location LIKE '%{location}%' ORDER BY water_usage DESC;"
            if "low compliance" in question or "lowest compliance" in question or "compliance scores below" in question:
                return f"SELECT name, location, compliance_score FROM suppliers WHERE location LIKE '%{location}%' AND compliance_score < 0.9 ORDER BY compliance_score ASC;"
            return f"SELECT name, location, latitude, longitude FROM suppliers WHERE location LIKE '%{location}%';"

        # Material-based queries without location
        if material:
            return f"SELECT p.name, s.name AS supplier, p.water_per_unit FROM products p JOIN suppliers s ON p.supplier_id = s.id WHERE p.material LIKE '%{material}%';"

        # Trend queries
        if "trend" in question or "historical" in question:
            if supplier_name:
                return f"SELECT s.name, sh.year, sh.carbon_footprint, sh.water_usage, sh.compliance_score FROM supplier_history sh JOIN suppliers s ON sh.supplier_id = s.id WHERE s.name = '{supplier_name}' ORDER BY sh.year;"

        # Other queries
        if "highest carbon footprint" in question:
            return "SELECT name, carbon_footprint FROM suppliers ORDER BY carbon_footprint DESC;"
        elif "highest water usage" in question:
            return "SELECT name, water_usage FROM suppliers ORDER BY water_usage DESC;"
        elif "lowest compliance" in question:
            return "SELECT name, compliance_score FROM suppliers ORDER BY compliance_score ASC;"
        elif "weather affect water-intensive" in question or "water-intensive products" in question:
            return "SELECT p.name, p.water_per_unit, s.name AS supplier FROM products p JOIN suppliers s ON p.supplier_id = s.id WHERE p.water_per_unit > 15;"
        elif "exceed compliance thresholds" in question or "compliance" in question:
            return "SELECT p.name, s.name AS supplier, s.compliance_score FROM products p JOIN suppliers s ON p.supplier_id = s.id WHERE s.compliance_score < 0.9;"
        elif "highest risk" in question:
            return "SELECT name, carbon_footprint, water_usage, compliance_score FROM suppliers;"

        # Fallback default query
        return "SELECT * FROM suppliers LIMIT 1;"

    def execute_query(self, query: str) -> List[Dict[str, Any]]:
        with self.engine.connect() as conn:
            result = conn.execute(text(query)).fetchall()
            columns = conn.execute(text(query)).keys()
            return [dict(zip(columns, row)) for row in result]

    def generate_insight(self, question: str, results: List[Dict[str, Any]], external_data: Dict[str, Any]) -> str:
        question = question.lower()
        # Extract location explicitly for use in insights
        location = "India" if "india" in question else "China" if "china" in question else "USA" if "usa" in question else "Bangladesh" if "bangladesh" in question else "Pakistan" if "pakistan" in question else "Italy" if "italy" in question else "unknown"
        
        if not results:
            if "suppliers in" in question and ("low compliance" in question or "lowest compliance" in question or "compliance scores below" in question):
                return f"No suppliers in {location} have compliance scores below the threshold of 0.9—Worldly can leverage this strength for ESG compliance."
            if "products in" in question and ("use" in question or "are made of" in question):
                material = "cotton" if "cotton" in question else "wool" if "wool" in question else "polyester" if "polyester" in question else "denim" if "denim" in question else "unknown"
                return f"No products in {location} use {material}—Worldly can explore alternative materials or regions."
            return "No data available to generate insight."

        # Product queries with materials
        if "products in" in question and ("use" in question or "are made of" in question):
            product = results[0]["name"]
            supplier = results[0]["supplier"]
            material = "cotton" if "cotton" in question else "wool" if "wool" in question else "polyester" if "polyester" in question else "denim" if "denim" in question else "unknown"
            weather = external_data["weather"][supplier]["condition"] if supplier in external_data["weather"] else "unknown"
            if "high water usage" in question:
                water_usage = results[0]["water_per_unit"]
                industry_avg = 15.0  # Example industry average for apparel products
                comparison = "above" if water_usage > industry_avg else "below"
                diff = ((water_usage - industry_avg) / industry_avg) * 100 if industry_avg != 0 else 0
                return f"{product} from {supplier} uses {material} and has high water usage at {water_usage} m³, {abs(diff):.1f}% {comparison} the industry average of {industry_avg} m³. Weather conditions ({weather}) may impact production—Worldly can explore sustainable alternatives to reduce water impact."
            if "low carbon footprint" in question:
                carbon_footprint = results[0]["carbon_per_unit"]
                industry_avg = 0.5  # Example industry average for apparel products
                comparison = "below" if carbon_footprint < industry_avg else "above"
                diff = ((carbon_footprint - industry_avg) / industry_avg) * 100 if industry_avg != 0 else 0
                return f"{product} from {supplier} uses {material} and has a low carbon footprint at {carbon_footprint} kg CO2e, {abs(diff):.1f}% {comparison} the industry average of {industry_avg} kg CO2e. Weather conditions ({weather}) may impact production—Worldly can highlight this for sustainable sourcing."
            return f"{product} from {supplier} uses {material}, which may have sustainability implications. Weather conditions ({weather}) may impact production—Worldly can assess its environmental impact."

        # Trend queries with dynamic metric selection
        if "trend" in question or "historical" in question:
            supplier_name = results[0]["name"] if "name" in results[0] else "Unknown"
            trends = results
            # Determine the metric to analyze based on the question
            metric = "carbon_footprint"
            metric_name = "carbon footprint"
            unit = "tons CO2e"
            if "water usage" in question:
                metric = "water_usage"
                metric_name = "water usage"
                unit = "m³"
            elif "compliance" in question:
                metric = "compliance_score"
                metric_name = "compliance score"
                unit = "score"
            
            trend = "decreasing" if trends[-1][metric] < trends[0][metric] else "increasing"
            trend_pct = self._calculate_trend_percentage(trends, metric)
            future = self._predict_future(trends, metric)
            return f"{supplier_name}’s {metric_name} is {trend} from {trends[0][metric]} in 2021 to {trends[-1][metric]} in 2024 ({trend_pct:.1f}% change). If trends continue, it may be {future:.1f} {unit} by 2025—Worldly can leverage this trend to meet client ESG goals."

        # Supplier queries with location and metric
        if "suppliers in" in question or "suppliers are in" in question or "suppliers located in" in question:
            supplier = results[0]["name"]
            if "highest carbon footprint" in question:
                with self.engine.connect() as conn:
                    supplier_id = conn.execute(text("SELECT id FROM suppliers WHERE name = :name"), {"name": supplier}).fetchone()[0]
                    trends = self._fetch_historical_trends(supplier_id)
                current_value = trends[-1]["carbon_footprint"]  # Use latest historical value (2024)
                trend = "decreasing" if trends[-1]["carbon_footprint"] < trends[0]["carbon_footprint"] else "increasing"
                future = self._predict_future(trends, "carbon_footprint")  # Predict 2025 based on 2021-2024 trend
                trend_pct = self._calculate_trend_percentage(trends, "carbon_footprint")
                if "china" in question:
                    return f"{supplier} in China has the highest carbon footprint at {current_value} tons CO2e in 2024, with a {trend} trend ({trend_pct:.1f}% since 2021). China’s strict emissions regulations may require Worldly’s Higg Index to accelerate reductions to {future:.1f} tons by 2025."
                elif "india" in question:
                    return f"{supplier} in India has the highest carbon footprint at {current_value} tons CO2e in 2024, with a {trend} trend ({trend_pct:.1f}% since 2021). India’s growing textile sector may benefit from Worldly’s Higg Index to reduce emissions to {future:.1f} tons by 2025."
                return f"{supplier}’s {current_value} tons CO2e in 2024 is the highest, with a {trend} trend ({trend_pct:.1f}% since 2021). If trends continue, it may drop to {future:.1f} tons by 2025—Worldly’s Higg Index can accelerate this."
            elif "highest water usage" in question:
                water_usage = results[0]["water_usage"]
                industry_avg = 15000.0
                diff = ((water_usage - industry_avg) / industry_avg) * 100 if industry_avg != 0 else 0
                comparison = "above" if water_usage > industry_avg else "below"
                return f"{supplier} has the highest water usage at {water_usage} m³, {abs(diff):.1f}% {comparison} the industry average of {industry_avg} m³—Worldly can target them for water reduction initiatives."
            elif "low compliance" in question or "lowest compliance" in question or "compliance scores below" in question:
                return f"{supplier} has the lowest compliance score at {results[0]['compliance_score']}—Worldly should prioritize an audit to improve ESG performance."
            return f"{supplier} is located in {location}, which may face regional sustainability challenges—Worldly can assess local impacts."

        # Other queries
        if "highest carbon footprint" in question:
            top_supplier = results[0]["name"]
            with self.engine.connect() as conn:
                supplier_id = conn.execute(text("SELECT id FROM suppliers WHERE name = :name"), {"name": top_supplier}).fetchone()[0]
                trends = self._fetch_historical_trends(supplier_id)
            current_value = trends[-1]["carbon_footprint"]  # Use latest historical value (2024)
            trend = "decreasing" if trends[-1]["carbon_footprint"] < trends[0]["carbon_footprint"] else "increasing"
            future = self._predict_future(trends, "carbon_footprint")
            trend_pct = self._calculate_trend_percentage(trends, "carbon_footprint")
            return f"{top_supplier}’s {current_value} tons CO2e in 2024 is the highest, with a {trend} trend ({trend_pct:.1f}% since 2021). If trends continue, it may drop to {future:.1f} tons by 2025—Worldly’s Higg Index can accelerate this."
        elif "highest water usage" in question:
            top_supplier = results[0]["name"]
            water_usage = results[0]["water_usage"]
            industry_avg = 15000.0
            diff = ((water_usage - industry_avg) / industry_avg) * 100 if industry_avg != 0 else 0
            comparison = "above" if water_usage > industry_avg else "below"
            return f"{top_supplier} has the highest water usage at {water_usage} m³, {abs(diff):.1f}% {comparison} the industry average of {industry_avg} m³—Worldly can target them for water reduction initiatives."
        elif "lowest compliance" in question:
            low_supplier = results[0]["name"]
            return f"{low_supplier} has the lowest compliance score at {results[0]['compliance_score']}—Worldly should prioritize an audit to improve ESG performance."
        elif "water-intensive" in question:
            supplier = results[0]["supplier"]
            weather = external_data["weather"][supplier]["condition"]
            return f"Worldly can flag water-intensive products from {supplier}, potentially delayed by {weather} conditions—consider sourcing from Patagonia Suppliers with lower risk."
        elif "compliance" in question:
            product = results[0]["name"]
            supplier = results[0]["supplier"]
            return f"{product} from {supplier} falls below Worldly’s 0.9 compliance threshold—recommend auditing their practices to meet client ESG standards."
        elif "highest risk" in question:
            top_supplier = results[0]["name"]
            risk_score = self._calculate_risk_score(results[0]["carbon_footprint"], results[0]["water_usage"], results[0]["compliance_score"])
            return f"{top_supplier} has the highest risk score of {risk_score:.1f}—Worldly should prioritize them for sustainability interventions."
        return "No specific insight generated."

    def generate_visualization(self, results: List[Dict[str, Any]], question: str) -> str:
        if not results:
            return None
        df = pd.DataFrame(results)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        worldly_colors = {
            "high_risk": "#D32F2F",
            "moderate_risk": "#FF9800",
            "low_risk": "#4CAF50",
            "shahjalal": "#0288D1",
            "patagonia": "#4CAF50",
            "marzotto": "#7B1FA2",
            "arvind": "#F57C00",
            "crystal": "#388E3C",
            "esquel": "#D81B60",
            "nishat": "#0288D1",
            "vardhman": "#7B1FA2",
            "below_threshold": "#D32F2F",
            "above_threshold": "#4CAF50"
        }

        question = question.lower()
        # Visualization for carbon footprint (suppliers)
        if "carbon_footprint" in df.columns and "name" in df.columns and "year" not in df.columns:
            external_data = self._fetch_external_data()
            df["emissions_risk"] = df["name"].map(lambda x: external_data["sustainability"][x]["emissions_risk"])
            df["color"] = df["emissions_risk"].map({
                "High": worldly_colors["high_risk"],
                "Moderate": worldly_colors["moderate_risk"],
                "Low": worldly_colors["low_risk"]
            })
            risk_data = self.execute_query("SELECT name, carbon_footprint, water_usage, compliance_score FROM suppliers")
            risk_df = pd.DataFrame(risk_data)
            risk_df["risk_score"] = risk_df.apply(lambda row: self._calculate_risk_score(row["carbon_footprint"], row["water_usage"], row["compliance_score"]), axis=1)
            df = df.merge(risk_df[["name", "risk_score"]], on="name")
            fig = px.bar(
                df,
                x="name",
                y="carbon_footprint",
                title=f"Carbon Footprint by Supplier ({question}) - Worldly ESG Insights",
                color="color",
                color_discrete_map="identity",
                labels={"carbon_footprint": "Carbon Footprint (tons CO2e)", "name": "Supplier"},
                hover_data=["risk_score"]
            )
            industry_avg = 1000.0
            fig.add_hline(y=industry_avg, line_dash="dash", line_color="gray", annotation_text="Industry Avg (1000 tons)", annotation_position="top left")
            high_risk = df[df["emissions_risk"] == "High"]
            if not high_risk.empty:
                fig.add_annotation(
                    x=high_risk.iloc[0]["name"],
                    y=high_risk.iloc[0]["carbon_footprint"],
                    text="High Risk",
                    showarrow=True,
                    arrowhead=1,
                    yshift=10,
                    font=dict(color=worldly_colors["high_risk"])
                )
            filename = f"worldly_carbon_viz_{timestamp}.html"

        # Visualization for water usage (suppliers)
        elif "water_usage" in df.columns and "name" in df.columns and "year" not in df.columns:
            external_data = self._fetch_external_data()
            df["water_risk"] = df["name"].map(lambda x: external_data["sustainability"][x]["water_risk"])
            df["color"] = df["water_risk"].map({
                "High": worldly_colors["high_risk"],
                "Moderate": worldly_colors["moderate_risk"],
                "Low": worldly_colors["low_risk"]
            })
            fig = px.bar(
                df,
                x="name",
                y="water_usage",
                title=f"Water Usage by Supplier ({question}) - Worldly ESG Insights",
                color="color",
                color_discrete_map="identity",
                labels={"water_usage": "Water Usage (cubic meters)", "name": "Supplier"},
                hover_data=["water_risk"]
            )
            industry_avg = 15000.0
            fig.add_hline(y=industry_avg, line_dash="dash", line_color="gray", annotation_text="Industry Avg (15000 m³)", annotation_position="top left")
            high_risk = df[df["water_risk"] == "High"]
            if not high_risk.empty:
                fig.add_annotation(
                    x=high_risk.iloc[0]["name"],
                    y=high_risk.iloc[0]["water_usage"],
                    text="High Risk",
                    showarrow=True,
                    arrowhead=1,
                    yshift=10,
                    font=dict(color=worldly_colors["high_risk"])
                )
            filename = f"worldly_water_usage_viz_{timestamp}.html"

        # Visualization for product queries with water_per_unit
        elif "water_per_unit" in df.columns and "name" in df.columns and "year" not in df.columns:
            df["color"] = df["supplier"].map({
                "Shahjalal Textile Mills": worldly_colors["shahjalal"],
                "Patagonia Suppliers": worldly_colors["patagonia"],
                "Marzotto Group": worldly_colors["marzotto"],
                "Arvind Limited": worldly_colors["arvind"],
                "Crystal Group": worldly_colors["crystal"],
                "Esquel Group": worldly_colors["esquel"],
                "Nishat Mills": worldly_colors["nishat"],
                "Vardhman Textiles": worldly_colors["vardhman"]
            })
            fig = px.bar(
                df,
                x="name",
                y="water_per_unit",
                title=f"Water Usage per Unit ({question}) - Worldly ESG Insights",
                color="color",
                color_discrete_map="identity",
                labels={"water_per_unit": "Water Usage (cubic meters)", "name": "Product"},
                hover_data=["supplier"]
            )
            industry_avg = 15.0
            fig.add_hline(y=industry_avg, line_dash="dash", line_color="gray", annotation_text="Industry Avg (15 m³)", annotation_position="top left")
            # Add weather risk annotation if applicable
            external_data = self._fetch_external_data()
            for _, row in df.iterrows():
                supplier = row["supplier"]
                weather = external_data["weather"][supplier]["condition"] if supplier in external_data["weather"] else None
                if weather and "rain" in weather.lower():
                    fig.add_annotation(
                        x=row["name"],
                        y=row["water_per_unit"],
                        text="Weather Risk (Rain)",
                        showarrow=True,
                        arrowhead=1,
                        yshift=10,
                        font=dict(color=worldly_colors["high_risk"])
                    )
            filename = f"worldly_water_viz_{timestamp}.html"

        # Visualization for product queries with carbon_per_unit
        elif "carbon_per_unit" in df.columns and "name" in df.columns and "year" not in df.columns:
            df["color"] = df["supplier"].map({
                "Shahjalal Textile Mills": worldly_colors["shahjalal"],
                "Patagonia Suppliers": worldly_colors["patagonia"],
                "Marzotto Group": worldly_colors["marzotto"],
                "Arvind Limited": worldly_colors["arvind"],
                "Crystal Group": worldly_colors["crystal"],
                "Esquel Group": worldly_colors["esquel"],
                "Nishat Mills": worldly_colors["nishat"],
                "Vardhman Textiles": worldly_colors["vardhman"]
            })
            fig = px.bar(
                df,
                x="name",
                y="carbon_per_unit",
                title=f"Carbon Footprint per Unit ({question}) - Worldly ESG Insights",
                color="color",
                color_discrete_map="identity",
                labels={"carbon_per_unit": "Carbon Footprint (kg CO2e)", "name": "Product"},
                hover_data=["supplier"]
            )
            industry_avg = 0.5
            fig.add_hline(y=industry_avg, line_dash="dash", line_color="gray", annotation_text="Industry Avg (0.5 kg CO2e)", annotation_position="top left")
            # Add weather risk annotation if applicable
            external_data = self._fetch_external_data()
            for _, row in df.iterrows():
                supplier = row["supplier"]
                weather = external_data["weather"][supplier]["condition"] if supplier in external_data["weather"] else None
                if weather and "rain" in weather.lower():
                    fig.add_annotation(
                        x=row["name"],
                        y=row["carbon_per_unit"],
                        text="Weather Risk (Rain)",
                        showarrow=True,
                        arrowhead=1,
                        yshift=10,
                        font=dict(color=worldly_colors["high_risk"])
                    )
            filename = f"worldly_carbon_per_unit_viz_{timestamp}.html"

        # Visualization for compliance scores
        elif "compliance_score" in df.columns and "location" in df.columns and "year" not in df.columns:
            df["status"] = df["compliance_score"].apply(lambda x: "Below Threshold" if x < 0.9 else "Above Threshold")
            df["color"] = df["status"].map({
                "Below Threshold": worldly_colors["below_threshold"],
                "Above Threshold": worldly_colors["above_threshold"]
            })
            fig = px.bar(
                df,
                x="name",
                y="compliance_score",
                title=f"Compliance Scores by Supplier ({question}) - Worldly ESG Insights",
                color="color",
                color_discrete_map="identity",
                labels={"compliance_score": "Compliance Score", "name": "Supplier"},
                hover_data=["location"]
            )
            fig.add_hline(y=0.9, line_dash="dash", line_color="red", annotation_text="Compliance Threshold (0.9)", annotation_position="top right")
            industry_avg = 0.92
            fig.add_hline(y=industry_avg, line_dash="dash", line_color="gray", annotation_text="Industry Avg (0.92)", annotation_position="top left")
            filename = f"worldly_compliance_viz_{timestamp}.html"

        # Visualization for trend queries with dynamic metric selection
        elif "year" in df.columns and ("carbon_footprint" in df.columns or "water_usage" in df.columns or "compliance_score" in df.columns):
            supplier_name = df["name"].iloc[0] if "name" in df.columns else "Unknown"
            # Determine the metric to plot based on the question
            metric = "carbon_footprint"
            metric_label = "Carbon Footprint (tons CO2e)"
            industry_avg = 1000.0
            industry_avg_label = "Industry Avg (1000 tons)"
            if "water usage" in question:
                metric = "water_usage"
                metric_label = "Water Usage (cubic meters)"
                industry_avg = 15000.0
                industry_avg_label = "Industry Avg (15000 m³)"
            elif "compliance" in question:
                metric = "compliance_score"
                metric_label = "Compliance Score"
                industry_avg = 0.92
                industry_avg_label = "Industry Avg (0.92)"

            fig = px.line(
                df,
                x="year",
                y=metric,
                title=f"{metric_label} Trend for {supplier_name} ({question}) - Worldly ESG Insights",
                labels={metric: metric_label, "year": "Year"},
                markers=True
            )
            fig.add_hline(y=industry_avg, line_dash="dash", line_color="gray", annotation_text=industry_avg_label, annotation_position="top left")
            filename = f"worldly_trend_viz_{timestamp}.html"

        # Visualization for supplier locations
        elif "location" in df.columns and "name" in df.columns and "latitude" in df.columns and "longitude" in df.columns:
            fig = px.scatter_geo(
                df,
                lat="latitude",
                lon="longitude",
                hover_name="name",
                hover_data=["location"],
                title=f"Suppliers by Location ({question}) - Worldly ESG Insights",
                projection="natural earth"
            )
            fig.update_geos(
                showcountries=True,
                countrycolor="Black",
                showland=True,
                landcolor="LightGreen",
                showocean=True,
                oceancolor="LightBlue"
            )
            filename = f"worldly_location_viz_{timestamp}.html"

        else:
            return None

        fig.update_layout(
            title_font_size=16,
            title_font_family="Arial",
            title_x=0.5,
            font=dict(family="Arial", size=12),
            plot_bgcolor="white",
            paper_bgcolor="white",
            showlegend=False,
            xaxis=dict(showgrid=True, gridcolor="lightgray") if "year" not in df.columns else dict(showgrid=True, gridcolor="lightgray"),
            yaxis=dict(showgrid=True, gridcolor="lightgray") if "year" not in df.columns else dict(showgrid=True, gridcolor="lightgray"),
            margin=dict(l=50, r=50, t=50, b=50)
        )
        fig.write_html(filename)
        return filename

    def run(self, question: str) -> Dict[str, Any]:
        cache_key = f"worldly:{hash(question)}"
        cached_result = self._get_cached_result(cache_key)
        if cached_result:
            return cached_result

        try:
            sql_query = self.generate_sql(question)
            if not self._validate_sql(sql_query):
                return {"error": "Invalid SQL generated.", "query": sql_query}

            results = self.execute_query(sql_query)
            # Always fetch external data, even if results are empty
            external_data = self._fetch_external_data()
            if not results:
                response = {
                    "message": "No data found.",
                    "query": sql_query,
                    "results": [],
                    "insight": self.generate_insight(question, results, external_data),
                    "visualization": self.generate_visualization(results, question),
                    "external_data_summary": {
                        "weather_conditions": {k: v["condition"] for k, v in external_data["weather"].items()},
                        "emissions_risks": {k: v["emissions_risk"] for k, v in external_data["sustainability"].items()}
                    }
                }
                self._cache_result(cache_key, response)
                return response

            insight = self.generate_insight(question, results, external_data)
            viz_file = self.generate_visualization(results, question)

            weather_summary = {k: v["condition"] for k, v in external_data["weather"].items()}
            sust_summary = {k: v["emissions_risk"] for k, v in external_data["sustainability"].items()}

            response = {
                "query": sql_query,
                "results": results,
                "insight": insight,
                "visualization": viz_file if viz_file else "No visualization generated.",
                "external_data_summary": {"weather_conditions": weather_summary, "emissions_risks": sust_summary}
            }
            self._cache_result(cache_key, response)
            return response

        except Exception as e:
            return {"error": str(e), "query": sql_query if 'sql_query' in locals() else None}

# Step 3: Interactive Demo for Worldly Interview
if __name__ == "__main__":
    print("Welcome to the Worldly Sustainability Risk Agent Demo - March 26, 2025")
    print("Built to enhance Worldly’s ESG transparency with real-world data and actionable insights")
    print("Enter a question about suppliers, products, or trends (or type 'exit' to quit)\n")
    initialize_sustainability_db()
    agent = WorldlySustainabilityAgent()

    while True:
        question = input("Your question: ")
        if not question.strip():  # Check for empty or whitespace-only input
            print("\nPlease enter a valid question or type 'exit' to quit.\n")
            continue
        if question.lower() == "exit":
            break
        print(f"\nQuestion: {question}")
        response = agent.run(question)
        print(f"SQL Query: {response.get('query')}")
        print(f"Results: {response.get('results')}")
        print(f"Insight: {response.get('insight', 'N/A')}")
        print(f"Visualization: {response.get('visualization')}")
        print(f"External Data Summary: {response.get('external_data_summary')}")
        if "error" in response:
            print(f"Error: {response['error']}")
        print("\n")