import os
from flask import Flask, request, render_template
from worldly_agent import WorldlySustainabilityAgent  # Import your agent
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
WEATHER_API_KEY = os.getenv("WEATHER_API_KEY")
if not WEATHER_API_KEY:
    raise ValueError("Missing WEATHER_API_KEY in environment variables.")

# Initialize Flask app
app = Flask(__name__)

# Initialize the Worldly agent
agent = WorldlySustainabilityAgent()

# Home route
@app.route("/", methods=["GET", "POST"])
def index():
    if request.method == "POST":
        question = request.form.get("question")
        if not question or question.strip().lower() == "exit":
            return render_template("index.html", error="Please enter a valid question.")
        
        # Run the agent
        response = agent.run(question)
        
        # Handle errors
        if "error" in response:
            return render_template("index.html", error=response["error"], query=response.get("query"))
        
        # Prepare results for display
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
    # For local testing
    app.run(debug=True, host="0.0.0.0", port=5000)
