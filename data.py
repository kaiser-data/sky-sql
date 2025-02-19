from sqlalchemy import create_engine, text
from contextlib import contextmanager
from typing import List, Dict, Any, Optional
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


class FlightData:
    """
    A class to handle flight data queries and database operations.

    Attributes:
        QUERIES (Dict[str, str]): Dictionary of SQL queries used in the class.
        _engine: SQLAlchemy engine for database connection.
    """

    # Centralized SQL Queries
    QUERIES = {
        "flight_by_id": """
            SELECT 
                flights.*, 
                airlines.airline AS Airline, 
                flights.ID AS FLIGHT_ID, 
                flights.DEPARTURE_DELAY AS DELAY
            FROM flights 
            JOIN airlines ON flights.AIRLINE = airlines.id 
            WHERE flights.ID = :id;
        """,
        "delayed_flights_by_airline": """
            SELECT 
                flights.*,
                airlines.airline AS Airline,
                flights.DEPARTURE_DELAY AS DELAY
            FROM flights
            JOIN airlines ON flights.AIRLINE = airlines.id
            WHERE airlines.airline = :airline AND flights.DEPARTURE_DELAY > 0;
        """,
        "delayed_flights_by_airport": """
            SELECT 
                flights.*,
                airlines.airline AS Airline,
                flights.DEPARTURE_DELAY AS DELAY
            FROM flights
            JOIN airlines ON flights.AIRLINE = airlines.id
            WHERE flights.ORIGIN_AIRPORT = :airport AND flights.DEPARTURE_DELAY > 0;
        """,
        "flights_by_date": """
            SELECT 
                flights.*,
                airlines.airline AS Airline,
                flights.DEPARTURE_DELAY AS DELAY
            FROM flights
            JOIN airlines ON flights.AIRLINE = airlines.id
            WHERE flights.DAY = :day AND flights.MONTH = :month AND flights.YEAR = :year;
        """,
        "delayed_by_airline": """
            WITH flight_stats AS (
                SELECT 
                    airlines.airline AS Airline,
                    COUNT(*) AS total_flights,
                    SUM(CASE WHEN flights.DEPARTURE_DELAY > 0 THEN 1 ELSE 0 END) AS delayed_flights
                FROM flights
                JOIN airlines ON flights.AIRLINE = airlines.id
                GROUP BY airlines.airline
            )
            SELECT 
                Airline,
                ROUND((CAST(delayed_flights AS FLOAT) * 100 / total_flights), 2) AS Percentage_Delayed
            FROM flight_stats
            ORDER BY Percentage_Delayed DESC;
        """,
        "delayed_by_hour": """
            SELECT 
                substr(SCHEDULED_DEPARTURE, 1, 2) AS ScheduledHour,
                COUNT(*) AS TotalFlights,
                SUM(CASE WHEN DEPARTURE_DELAY > 0 THEN 1 ELSE 0 END) AS DelayedFlights,
                ROUND(
                    (SUM(CASE WHEN DEPARTURE_DELAY > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*)), 
                    2
                ) AS Percentage_Delayed
            FROM flights
            GROUP BY ScheduledHour
            ORDER BY ScheduledHour;
        """,
        "delayed_by_route": """
            SELECT 
                ORIGIN_AIRPORT,
                DESTINATION_AIRPORT,
                COUNT(*) AS TotalFlights,
                SUM(CASE WHEN DEPARTURE_DELAY > 0 THEN 1 ELSE 0 END) AS DelayedFlights,
                ROUND(
                    (SUM(CASE WHEN DEPARTURE_DELAY > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*)), 
                    2
                ) AS Percentage_Delayed
            FROM flights
            GROUP BY ORIGIN_AIRPORT, DESTINATION_AIRPORT
            ORDER BY ORIGIN_AIRPORT, DESTINATION_AIRPORT;
        """,
        "airport_coordinates": """
            SELECT IATA_CODE, LATITUDE, LONGITUDE FROM airports;
        """
    }

    def __init__(self, db_uri: str):
        """
        Initialize the FlightData class with a database URI.

        Args:
            db_uri (str): The database connection URI.

        Raises:
            Exception: If the database connection fails.
        """
        try:
            self._engine = create_engine(db_uri, pool_pre_ping=True)
            self._test_connection()
        except Exception as e:
            logging.error(f"Failed to initialize database connection: {e}")
            raise

    @contextmanager
    def _get_connection(self):
        """
        Context manager for managing database connections.

        Yields:
            Connection: A database connection object.

        Raises:
            Exception: If there is an issue establishing or closing the connection.
        """
        connection = None
        try:
            connection = self._engine.connect()
            yield connection
        except Exception as e:
            logging.error(f"Database connection error: {e}")
            raise
        finally:
            if connection:
                connection.close()

    def _test_connection(self) -> None:
        """
        Test the database connection by executing a simple query.

        Raises:
            Exception: If the test query fails.
        """
        with self._get_connection() as conn:
            conn.execute(text("SELECT 1"))

    def _execute_query(self, query: str, params: Dict[str, Any] = None) -> List[Dict[str, Any]]:
        """
        Execute a SQL query with parameters.

        Args:
            query (str): The SQL query to execute.
            params (Dict[str, Any], optional): Query parameters. Defaults to None.

        Returns:
            List[Dict[str, Any]]: Query results as a list of dictionaries.

        Raises:
            Exception: If the query execution fails.
        """
        params = params or {}
        try:
            with self._get_connection() as connection:
                result = connection.execute(text(query), params)
                return [dict(row._mapping) for row in result]
        except Exception as e:
            logging.error(f"Query execution error: {e}\nQuery: {query}\nParams: {params}")
            return []

    # Public Methods

    def get_flight_by_id(self, flight_id: int) -> Optional[Dict[str, Any]]:
        """
        Retrieve flight information by ID.

        Args:
            flight_id (int): The ID of the flight to retrieve.

        Returns:
            Optional[Dict[str, Any]]: Flight details as a dictionary, or None if not found.
        """
        results = self._execute_query(self.QUERIES["flight_by_id"], {"id": flight_id})
        return results[0] if results else None

    def get_delayed_flights_by_airline(self, airline: str) -> List[Dict[str, Any]]:
        """
        Get delayed flights for a specific airline.

        Args:
            airline (str): The name of the airline.

        Returns:
            List[Dict[str, Any]]: A list of delayed flights as dictionaries.
        """
        return self._execute_query(self.QUERIES["delayed_flights_by_airline"], {"airline": airline})

    def get_delayed_flights_by_airport(self, airport: str) -> List[Dict[str, Any]]:
        """
        Get delayed flights for a specific origin airport.

        Args:
            airport (str): The IATA code of the origin airport.

        Returns:
            List[Dict[str, Any]]: A list of delayed flights as dictionaries.
        """
        return self._execute_query(self.QUERIES["delayed_flights_by_airport"], {"airport": airport})

    def get_flights_by_date(self, day: int, month: int, year: int) -> List[Dict[str, Any]]:
        """
        Get all flights for a specific date.

        Args:
            day (int): The day of the flight.
            month (int): The month of the flight.
            year (int): The year of the flight.

        Returns:
            List[Dict[str, Any]]: A list of flights as dictionaries.
        """
        return self._execute_query(self.QUERIES["flights_by_date"], {"day": day, "month": month, "year": year})

    def get_delayed_flights_percentage_by_airline(self) -> List[Dict[str, Any]]:
        """
        Get the percentage of delayed flights by airline.

        Returns:
            List[Dict[str, Any]]: A list of airline delay percentages as dictionaries.
        """
        return self._execute_query(self.QUERIES["delayed_by_airline"])

    def get_delayed_flights_percentage_by_hour(self) -> List[Dict[str, Any]]:
        """
        Get the percentage of delayed flights by hour interval.

        Returns:
            List[Dict[str, Any]]: A list of hourly delay percentages as dictionaries.
        """
        return self._execute_query(self.QUERIES["delayed_by_hour"])

    def get_delayed_flights_percentage_by_route(self) -> List[Dict[str, Any]]:
        """
        Get the percentage of delayed flights by route.

        Returns:
            List[Dict[str, Any]]: A list of route delay percentages as dictionaries.
        """
        return self._execute_query(self.QUERIES["delayed_by_route"])

    def get_airport_coordinates(self) -> List[Dict[str, Any]]:
        """
        Fetch airport coordinates with validation.

        Returns:
            List[Dict[str, Any]]: A list of valid airport coordinates as dictionaries.
        """
        results = self._execute_query(self.QUERIES["airport_coordinates"])

        valid_airports = []
        for row in results:
            try:
                latitude = float(row["LATITUDE"])
                longitude = float(row["LONGITUDE"])
                valid_airports.append({
                    "IATA": row["IATA_CODE"],
                    "Latitude": latitude,
                    "Longitude": longitude
                })
            except ValueError:
                logging.warning(
                    f"Invalid coordinates for airport {row['IATA_CODE']}: LATITUDE={row['LATITUDE']}, LONGITUDE={row['LONGITUDE']}"
                )
        return valid_airports