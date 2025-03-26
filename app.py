import os
from flask import Flask, request, render_template
from worldly_agent import WorldlySustainabilityAgent
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
WEATHER_API_KEY = os.getenv("WEATHER_API_KEY")
if not WEATHER_API_KEY:
    raise ValueError("Missing WEATHER_API_KEY in environment variables.")

app = Flask(__name__)
agent = WorldlySustainabilityAgent(db_path="/tmp/worldly_risk.db")

@app.route("/", methods=["GET", "POST"])
def index():
    if request.method == "POST":
        question = request.form.get("question")
        if not question or question.strip().lower() == "exit":
            return render_template("index.html", error="Please enter a valid question.")
        
        response = agent.run(question)
        
        if "error" in response:
            return render_template("index.html", error=response["error"], query=response.get("query"))
        
        results = {
            "query": response.get("query"),
            "results": response.get("results"),
            "insight": response.get("insight", "N/A"),
            "visualization": response.get("visualization"),
            "external_data": response.get("external_data_summary")
        }
        return render_template("index.html", results=results, question=question)
    
    return render_template("index.html")

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(debug=False, host="0.0.0.0", port=port)