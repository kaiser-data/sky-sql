import seaborn as sns
import matplotlib.pyplot as plt
import pandas as pd
from datetime import datetime
import logging
from typing import Callable, Dict, List, Any, Tuple
from dataclasses import dataclass
import sys
from pathlib import Path
import geopandas as gpd
from shapely.geometry import LineString
import os



# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('flight_analysis.log')
    ]
)

SQLITE_URI = 'sqlite:///data/flights.sqlite3'
IATA_LENGTH = 3
PLOT_OUTPUT_DIR = Path('plots')


@dataclass
class PlotConfig:
    """Configuration for plot generation."""
    figsize: Tuple[int, int]
    title: str
    xlabel: str
    ylabel: str
    rotation: int
    palette: str
    output_file: str


class InputValidator:
    """Handles input validation for various data types."""

    @staticmethod
    def get_valid_input(prompt: str, validator: Callable[[str], bool], 
                        error_msg: str = "Invalid input. Try again...") -> str:
        """Get and validate user input."""
        while True:
            user_input = input(prompt).strip()
            try:
                if validator(user_input):
                    return user_input
                print(error_msg)
            except Exception as e:
                logging.error(f"Input validation error: {e}")
                print(error_msg)

    @staticmethod
    def validate_iata(code: str) -> bool:
        """Validate IATA airport code."""
        return code.isalpha() and len(code) == IATA_LENGTH

    @staticmethod
    def validate_date(date_str: str) -> bool:
        """Validate date string."""
        try:
            datetime.strptime(date_str, '%d/%m/%Y')
            return True
        except ValueError:
            return False

    @staticmethod
    def validate_flight_id(id_str: str) -> bool:
        """Validate flight ID."""
        try:
            flight_id = int(id_str)
            return flight_id > 0
        except ValueError:
            return False


class FlightDataVisualizer:
    """Handles visualization of flight data."""

    def __init__(self):
        """Initialize visualizer and create output directory."""
        PLOT_OUTPUT_DIR.mkdir(exist_ok=True)
        self.set_plot_style()

    @staticmethod
    def set_plot_style():
        """Set default plot style."""
        plt.style.use('default')  # Use default matplotlib style
        sns.set_theme(style="whitegrid")  # Set seaborn theme
        sns.set_context("talk", font_scale=0.9)
        plt.rcParams['figure.figsize'] = [12, 6]
        plt.rcParams['figure.dpi'] = 100

    def create_plot(self, df: pd.DataFrame, config: PlotConfig) -> None:
        """Create and save a plot with given configuration."""
        try:
            plt.figure(figsize=config.figsize)

            if 'Interval' in df.columns:
                sns.barplot(x="Interval", y="Percentage_Delayed", data=df, palette=config.palette)
            else:
                sns.barplot(x="Airline", y="Percentage_Delayed", data=df, palette=config.palette)

            plt.title(config.title, pad=20)
            plt.xlabel(config.xlabel)
            plt.ylabel(config.ylabel)
            plt.xticks(rotation=config.rotation, ha='right')
            plt.tight_layout()

            output_path = PLOT_OUTPUT_DIR / config.output_file
            plt.savefig(output_path, dpi=300, bbox_inches='tight')
            logging.info(f"Plot saved to {output_path}")
            plt.show()
            plt.close()
        except Exception as e:
            logging.error(f"Error creating plot: {e}")
            print("Failed to create plot. Check the log file for details.")

    def create_heatmap(self, df: pd.DataFrame, config: PlotConfig) -> None:
        """Create and save a heatmap with given configuration."""
        try:
            plt.figure(figsize=config.figsize)

            sns.heatmap(
                df,
                annot=False,  # Disable cell annotations (no numbers)
                cmap="Reds",  # Use only the red color spectrum
                cbar_kws={"label": "Percentage Delayed (%)"},  # Color bar label
                linewidths=0.5,  # Add grid lines for better readability
                linecolor='white'  # Set grid line color to white
            )

            plt.title(config.title, pad=20)
            plt.xlabel(config.xlabel)
            plt.ylabel(config.ylabel)
            plt.xticks(rotation=config.rotation, ha='right')
            plt.tight_layout()

            output_path = PLOT_OUTPUT_DIR / config.output_file
            plt.savefig(output_path, dpi=300, bbox_inches='tight')
            logging.info(f"Heatmap saved to {output_path}")
            plt.show()
            plt.close()
        except Exception as e:
            logging.error(f"Error creating heatmap: {e}")
            print("Failed to create heatmap. Check the log file for details.")


class FlightAnalyzer:
    """Main class for flight data analysis."""

    def __init__(self, data_manager: Any):
        """Initialize analyzer with data manager."""
        self.data_manager = data_manager
        self.visualizer = FlightDataVisualizer()
        self.validator = InputValidator()

    def print_results(self, results: List[Dict[str, Any]]) -> None:
        """Print flight results in a formatted manner."""
        if not results:
            print("No results found.")
            return

        print(f"Found {len(results)} results:")
        for result in results:
            try:
                delay = int(result.get('DEPARTURE_DELAY', 0) or 0)
                flight_info = (f"{result['ID']}. {result['ORIGIN_AIRPORT']} -> "
                               f"{result['DESTINATION_AIRPORT']} by {result['AIRLINE']}")
                if delay > 0:
                    flight_info += f", Delay: {delay} Minutes"
                print(flight_info)
            except (ValueError, KeyError) as e:
                logging.error(f"Error formatting result: {e}")
                continue

    def delayed_flights_by_airline(self) -> None:
        """Handle delayed flights by airline query."""
        airline = self.validator.get_valid_input(
            "Enter airline name: ",
            lambda x: bool(x.strip())
        )
        results = self.data_manager.get_delayed_flights_by_airline(airline)
        self.print_results(results)

    def delayed_flights_by_airport(self) -> None:
        """Handle delayed flights by airport query."""
        airport = self.validator.get_valid_input(
            "Enter origin airport IATA code: ",
            self.validator.validate_iata,
            "Invalid IATA code. Must be 3 letters."
        ).upper()
        results = self.data_manager.get_delayed_flights_by_airport(airport)
        self.print_results(results)

    def flight_by_id(self) -> None:
        """Handle flight by ID query."""
        flight_id = int(self.validator.get_valid_input(
            "Enter flight ID: ",
            self.validator.validate_flight_id,
            "Invalid flight ID. Must be a positive integer."
        ))
        results = self.data_manager.get_flight_by_id(flight_id)
        self.print_results([results] if results else [])

    def flights_by_date(self) -> None:
        """Handle flights by date query."""
        date_str = self.validator.get_valid_input(
            "Enter date (DD/MM/YYYY): ",
            self.validator.validate_date,
            "Invalid date format. Use DD/MM/YYYY."
        )
        date = datetime.strptime(date_str, '%d/%m/%Y')
        results = self.data_manager.get_flights_by_date(date.day, date.month, date.year)
        self.print_results(results)

    def plot_delayed_flights_by_airline(self) -> None:
        """Create plot of delayed flights by airline."""
        results = self.data_manager.get_delayed_flights_percentage_by_airline()
        if not results:
            print("No data available for plotting.")
            return

        df = pd.DataFrame(results).sort_values(by="Percentage_Delayed", ascending=True)
        config = PlotConfig(
            figsize=(12, 6),
            title="Percentage of Delayed Flights by Airline",
            xlabel="Airline",
            ylabel="Percentage Delayed (%)",
            rotation=45,
            palette="coolwarm",
            output_file="delayed_flights_by_airline.png"
        )
        self.visualizer.create_plot(df, config)

    def plot_delayed_flights_by_hour(self) -> None:
        """Create plot of delayed flights by hour."""
        results = self.data_manager.get_delayed_flights_percentage_by_hour()
        if not results:
            print("No data available for plotting.")
            return

        df = pd.DataFrame(results)
        df.rename(columns={"ScheduledHour": "Interval", "Percentage_Delayed": "Percentage_Delayed"}, inplace=True)

        config = PlotConfig(
            figsize=(16, 8),
            title="Percentage of Delayed Flights by Hour",
            xlabel="Time Interval (HH)",
            ylabel="Percentage Delayed (%)",
            rotation=45,
            palette="coolwarm_r",
            output_file="delayed_flights_by_hour.png"
        )
        self.visualizer.create_plot(df, config)

    def plot_delayed_flights_by_route(self) -> None:
        """Create heatmap of delayed flights by origin-destination pair."""
        results = self.data_manager.get_delayed_flights_percentage_by_route()
        if not results:
            print("No data available for plotting.")
            return

        df = pd.DataFrame(results)
        pivot_df = df.pivot(index="ORIGIN_AIRPORT", columns="DESTINATION_AIRPORT", values="Percentage_Delayed")
        pivot_df = pivot_df.fillna(0)

        config = PlotConfig(
            figsize=(16, 12),
            title="Percentage of Delayed Flights by Route",
            xlabel="Destination Airport",
            ylabel="Origin Airport",
            rotation=45,
            palette="Reds",
            output_file="delayed_flights_by_route_heatmap.png"
        )
        self.visualizer.create_heatmap(pivot_df, config)

    def plot_delayed_flights_by_route_map(self) -> None:
        """Create a geographic map showing delayed flights per route with colored lines."""
        try:
            # Fetch delayed flights percentage by route
            results = self.data_manager.get_delayed_flights_percentage_by_route()
            if not results:
                print("No data available for plotting.")
                return

            # Convert results to DataFrame
            df = pd.DataFrame(results)

            # Merge with airport coordinates
            airports = self.data_manager.get_airport_coordinates()
            airports_df = pd.DataFrame(airports)

            # Merge origin and destination airports with their coordinates
            df = pd.merge(df, airports_df, left_on="ORIGIN_AIRPORT", right_on="IATA", how="left")
            df.rename(columns={"Latitude": "Origin_Lat", "Longitude": "Origin_Lon"}, inplace=True)
            df = pd.merge(df, airports_df, left_on="DESTINATION_AIRPORT", right_on="IATA", how="left")
            df.rename(columns={"Latitude": "Dest_Lat", "Longitude": "Dest_Lon"}, inplace=True)

            # Drop rows with missing coordinates
            df = df.dropna(subset=["Origin_Lat", "Origin_Lon", "Dest_Lat", "Dest_Lon"])

            # Create geometry for lines
            df['geometry'] = df.apply(
                lambda row: LineString([(row['Origin_Lon'], row['Origin_Lat']), (row['Dest_Lon'], row['Dest_Lat'])]),
                axis=1
            )

            # Convert to GeoDataFrame
            gdf = gpd.GeoDataFrame(df, geometry='geometry')

            # Define color mapping for delay percentages
            def get_color(delay_percent):
                if delay_percent <= 25:
                    return '#FFCCCB'  # Very light pink for low delays
                elif delay_percent <= 50:
                    return '#FF6347'  # Tomato for medium-low delays
                elif delay_percent <= 75:
                    return '#DC143C'  # Crimson for medium-high delays
                else:
                    return '#8B0000'  # Dark red for high delays

            gdf['color'] = gdf['Percentage_Delayed'].apply(get_color)

            # --- Updated: Use the exact file path provided ---
            shapefile_path = "data/naturalearth_lowres/ne_10m_admin_0_countries.shp"
            
            # Check if the shapefile exists
            if not os.path.exists(shapefile_path):
                logging.error(f"Shapefile not found at {shapefile_path}")
                print(f"Could not find shapefile at {shapefile_path}")
                return

            # Load the shapefile
            try:
                world = gpd.read_file(shapefile_path)
                logging.info(f"Successfully loaded shapefile from {shapefile_path}")
            except Exception as e:
                logging.error(f"Error loading shapefile: {e}")
                print(f"Error loading shapefile: {e}")
                return

    # Define delay ranges
            delay_ranges = [(0, 25), (26, 50), (51, 75), (76, 100)]
            colors = ['#FFCCCB', '#FF6347', '#DC143C', '#8B0000']
            titles = ['0-25% Delay', '26-50% Delay', '51-75% Delay', '76-100% Delay']

            # Create a figure with subplots
            fig, axs = plt.subplots(4, 1, figsize=(20, 20))
            axs = axs.ravel()  # Flatten the array for easier indexing

            for i, (min_delay, max_delay) in enumerate(delay_ranges):
                # Filter the data for this delay range
                filtered_gdf = gdf[(gdf['Percentage_Delayed'] > min_delay) & (gdf['Percentage_Delayed'] <= max_delay)]
                
                # Plot the map
                world.plot(ax=axs[i], color='lightgrey', edgecolor='black')

                # Plot routes
                for _, row in filtered_gdf.iterrows():
                    axs[i].plot(
                        [row['geometry'].coords[0][0], row['geometry'].coords[1][0]],
                        [row['geometry'].coords[0][1], row['geometry'].coords[1][1]],
                        color=colors[i],
                        linewidth=1.5,
                        alpha=0.8
                    )

                # Configure each subplot
                axs[i].set_title(titles[i], fontsize=14)
                axs[i].set_xlim(xmin=-160, xmax=-50)
                axs[i].set_ylim(ymin=15, ymax=50)
                axs[i].set_aspect('equal')

            # Adjust layout to prevent overlapping
            plt.tight_layout()

            # Save and display the plot
            output_path = PLOT_OUTPUT_DIR / "delayed_flights_by_delay_range.png"
            plt.savefig(output_path, dpi=300, bbox_inches='tight')
            logging.info(f"Maps saved to {output_path}")
            plt.show()
            plt.close()



        except Exception as e:
            logging.error(f"Error creating geographic map: {e}")
            print("Failed to create geographic map. Check the log file for details.")


def create_menu() -> Dict[int, Tuple[Callable, str]]:
    """Create menu options mapping."""
    return {
        1: (FlightAnalyzer.flight_by_id, "Show flight by ID"),
        2: (FlightAnalyzer.flights_by_date, "Show flights by date"),
        3: (FlightAnalyzer.delayed_flights_by_airline, "Delayed flights by airline"),
        4: (FlightAnalyzer.delayed_flights_by_airport, "Delayed flights by airport"),
        5: (FlightAnalyzer.plot_delayed_flights_by_airline, "Plot delayed flights by airline"),
        6: (FlightAnalyzer.plot_delayed_flights_by_hour, "Plot delayed flights by hour"),
        7: (FlightAnalyzer.plot_delayed_flights_by_route, "Plot delayed flights by route (heatmap)"),
        8: (FlightAnalyzer.plot_delayed_flights_by_route_map, "Plot delayed flights by route (geographic map)"),
        9: (sys.exit, "Exit")
    }


def main():
    """Main program entry point."""
    try:
        from data import FlightData
        analyzer = FlightAnalyzer(FlightData(SQLITE_URI))
        menu = create_menu()

        while True:
            print("\nFlight Analysis Menu:")
            for key, (_, description) in menu.items():
                print(f"{key}. {description}")

            choice = InputValidator.get_valid_input(
                "\nEnter your choice (1-9): ",
                lambda x: x.isdigit() and 1 <= int(x) <= 9,
                "Invalid choice. Please enter a number between 1 and 9."
            )

            if int(choice) == 9:
                print("Goodbye!")
                sys.exit(0)
            menu[int(choice)][0](analyzer)
    except Exception as e:
        logging.error(f"Application error: {e}")
        print("An error occurred. Check the log file for details.")
        sys.exit(1)


if __name__ == "__main__":
    main()