# Sustainability Risk Agent

[Sustainability Risk Agent Demo]
*Live Demo: [https://worldly-demo.onrender.com](https://worldly-demo.onrender.com)*  
*GitHub Repository: [https://github.com/jaredmarko/worldly-demo](https://github.com/jaredmarko/worldly-demo)*

## Overview

The **Sustainability Risk Agent** is a web application designed to support Worldly’s mission of ESG (Environmental, Social, Governance) transparency in supply chains. Built as a proof-of-concept, the app allows users to query sustainability data for suppliers and products, providing actionable insights and interactive visualizations to drive data-driven decisions. It analyzes key ESG metrics like carbon footprints and water usage, predicts future trends, and integrates external weather data to add context, such as potential production impacts.

While it’s a prototype with a small dataset, it’s designed to be extensible for real-world applications.

---

## Features

- **Natural Language Query Processing**: Users can ask questions like “Which suppliers in China have the highest carbon footprint?” and the app generates corresponding SQL queries.
- **Actionable Insights**: Provides insights with trend analysis, such as percentage changes and future predictions (e.g., “Crystal Group’s carbon footprint is decreasing by 11.5% since 2021, predicting 1100 tons by 2025”).
- **Interactive Visualizations**: Uses Plotly to create bar charts, line charts, and scatter plots, embedded via iframes for easy interpretation.
- **External Data Integration**: Fetches weather data via the OpenWeatherMap API to add context, like potential production delays due to weather conditions.
- **Responsive Frontend**: Built with Bootstrap for a clean, user-friendly interface accessible on all devices.
- **Edge Case Handling**: Gracefully handles queries with no results, providing meaningful suggestions (e.g., “No products in Bangladesh use wool—Worldly can explore alternative materials or regions”).

---

## Tech Stack

- **Backend**: Flask (Python) for routing and serving the frontend.
- **Data Layer**: SQLite with SQLAlchemy for database management and dynamic SQL generation.
- **Data Analysis**: Pandas for data manipulation and trend analysis.
- **Visualization**: Plotly for interactive charts (bar, line, scatter).
- **Frontend**: Bootstrap for a responsive, professional UI.
- **External Data**: OpenWeatherMap API for weather data integration.
- **Deployment**: Render for cloud hosting, with Gunicorn as the WSGI server.

---

## Installation and Setup

### Prerequisites
- Python 3.10+
- Git
- An OpenWeatherMap API key (sign up at [https://openweathermap.org/](https://openweathermap.org/))

### Steps
1. **Clone the Repository**:
  
   git clone https://github.com/jaredmarko/worldly-demo.git
   cd worldly-demo
