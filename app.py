# Import necessary modules from Flask for creating the web application
from flask import Flask, jsonify, request, abort

# Import the FlightData class from the 'data' module, which handles database operations
from data import FlightData

# Initialize the Flask application
app = Flask(__name__)

# Create an instance of the FlightData class, providing the database URI as an argument.
# Replace 'sqlite:///data/flights.sqlite3' with the actual path to your SQLite database file.
flight_data = FlightData('sqlite:///data/flights.sqlite3')


# Define an endpoint to retrieve flight information by ID
@app.route('/flights/<int:flight_id>', methods=['GET'])
def get_flight_by_id(flight_id):
    """
    This endpoint retrieves a specific flight by its unique ID.

    Args:
        flight_id (int): The ID of the flight to retrieve.

    Returns:
        JSON response: If the flight exists, it returns the flight details in JSON format.
                       If the flight does not exist, it returns a 404 error.
    """
    flight = flight_data.get_flight_by_id(flight_id)  # Fetch the flight using the FlightData method
    if not flight:  # If no flight is found with the given ID
        abort(404, description="Flight not found")  # Return a 404 error
    return jsonify(flight)  # Return the flight details as JSON


# Define an endpoint to retrieve delayed flights for a specific airline
@app.route('/flights/delayed/airline/<string:airline>', methods=['GET'])
def get_delayed_flights_by_airline(airline):
    """
    This endpoint retrieves all delayed flights for a specific airline.

    Args:
        airline (str): The name of the airline.

    Returns:
        JSON response: If delayed flights are found, it returns them in JSON format.
                       If no delayed flights are found, it returns a message indicating so.
    """
    flights = flight_data.get_delayed_flights_by_airline(airline)  # Fetch delayed flights for the airline
    if not flights:  # If no delayed flights are found
        return jsonify({"message": "No delayed flights found for this airline"}), 200  # Return a success message
    return jsonify(flights)  # Return the list of delayed flights as JSON


# Define an endpoint to retrieve delayed flights for a specific origin airport
@app.route('/flights/delayed/airport/<string:airport>', methods=['GET'])
def get_delayed_flights_by_airport(airport):
    """
    This endpoint retrieves all delayed flights departing from a specific airport.

    Args:
        airport (str): The IATA code of the origin airport.

    Returns:
        JSON response: If delayed flights are found, it returns them in JSON format.
                       If no delayed flights are found, it returns a message indicating so.
    """
    flights = flight_data.get_delayed_flights_by_airport(airport)  # Fetch delayed flights for the airport
    if not flights:  # If no delayed flights are found
        return jsonify({"message": "No delayed flights found for this airport"}), 200  # Return a success message
    return jsonify(flights)  # Return the list of delayed flights as JSON


# Define an endpoint to retrieve all flights for a specific date
@app.route('/flights/date', methods=['GET'])
def get_flights_by_date():
    """
    This endpoint retrieves all flights for a specific date.

    Query Parameters:
        day (int): The day of the flight.
        month (int): The month of the flight.
        year (int): The year of the flight.

    Returns:
        JSON response: If flights are found, it returns them in JSON format.
                       If no flights are found, it returns a message indicating so.
                       If required parameters are missing, it returns a 400 error.
    """
    day = request.args.get('day', type=int)  # Get the 'day' query parameter
    month = request.args.get('month', type=int)  # Get the 'month' query parameter
    year = request.args.get('year', type=int)  # Get the 'year' query parameter

    # Ensure all required parameters are provided
    if not all([day, month, year]):
        abort(400,
              description="Parameters 'day', 'month', and 'year' are required")  # Return a 400 error if any are missing

    flights = flight_data.get_flights_by_date(day, month, year)  # Fetch flights for the specified date
    if not flights:  # If no flights are found
        return jsonify({"message": "No flights found for this date"}), 200  # Return a success message
    return jsonify(flights)  # Return the list of flights as JSON


# Define an endpoint to retrieve the percentage of delayed flights by airline
@app.route('/flights/delayed/percentage/airline', methods=['GET'])
def get_delayed_flights_percentage_by_airline():
    """
    This endpoint retrieves the percentage of delayed flights for each airline.

    Returns:
        JSON response: A list of airlines with their respective delay percentages.
    """
    result = flight_data.get_delayed_flights_percentage_by_airline()  # Fetch delay percentages by airline
    return jsonify(result)  # Return the results as JSON


# Define an endpoint to retrieve the percentage of delayed flights by hour interval
@app.route('/flights/delayed/percentage/hour', methods=['GET'])
def get_delayed_flights_percentage_by_hour():
    """
    This endpoint retrieves the percentage of delayed flights for each hour interval.

    Returns:
        JSON response: A list of hourly intervals with their respective delay percentages.
    """
    result = flight_data.get_delayed_flights_percentage_by_hour()  # Fetch delay percentages by hour
    return jsonify(result)  # Return the results as JSON


# Define an endpoint to retrieve the percentage of delayed flights by route
@app.route('/flights/delayed/percentage/route', methods=['GET'])
def get_delayed_flights_percentage_by_route():
    """
    This endpoint retrieves the percentage of delayed flights for each route.

    Returns:
        JSON response: A list of routes with their respective delay percentages.
    """
    result = flight_data.get_delayed_flights_percentage_by_route()  # Fetch delay percentages by route
    return jsonify(result)  # Return the results as JSON


# Define an endpoint to retrieve airport coordinates
@app.route('/airports/coordinates', methods=['GET'])
def get_airport_coordinates():
    """
    This endpoint retrieves the coordinates (latitude and longitude) of all airports.

    Returns:
        JSON response: A list of airports with their coordinates.
    """
    result = flight_data.get_airport_coordinates()  # Fetch airport coordinates
    return jsonify(result)  # Return the results as JSON


# Run the Flask application if this script is executed directly
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001, debug=True)  # Start the Flask server on host '0.0.0.0' and port 5001