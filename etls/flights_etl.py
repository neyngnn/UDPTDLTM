
import pandas as pd
import datetime
import requests
import json
import copy
import sys
import os

root_dir = os.getcwd()

def get_api_request(
    engine: str,
    departure_id: str,
    arrival_id: str,
    outbound_date: str,
    return_date: str = None,
    currency: str = 'VND',
    hl: str = 'vi',
    show_hidden: str = 'true'
) -> dict:
    url = "https://serpapi.com/search.json"
    params = dict(
        engine=engine,
        departure_id=departure_id,
        arrival_id=arrival_id,
        outbound_date=outbound_date,
        return_date=return_date,
        currency=currency,
        hl=hl,
        api_key='3fb86cc5490cf6c3fc50aeb5ee61c0204b073657da6cdc9daa5337d38dbde2f7',
    )

    response = requests.get(url, params)
    
    if response.status_code != 200:
        raise Exception(f"API request failed with status code {response.status_code}: {response.text}")

    return response.json()



def extract_flights_information(branch_dir: str, flight_information: dict) -> dict:
    """
    Lưu dữ liệu thô (raw data) được trích xuất dưới dạng tệp JSON cho mỗi ngày và chuyến bay.
    """
    # Tạo các thông tin cần thiết cho cấu trúc thư mục đơn giản hơn
    outbound_date = flight_information['outbound_date']  # Định dạng "YYYY-MM-DD"
    departure_id = flight_information['departure_id']
    arrival_id = flight_information['arrival_id']
    flight_folder_name = f"{departure_id}-{arrival_id}_{outbound_date}"  # Sử dụng mã sân bay và ngày làm tên thư mục

    # Gửi yêu cầu API để lấy thông tin chuyến bay
    response = get_api_request(**flight_information)

    # Kiểm tra phản hồi từ API và đổi tên khóa nếu có
    if 'best_flights' in response:
        response['best_trips'] = response.pop('best_flights')
    if 'other_flights' in response:
        response['other_trips'] = response.pop('other_flights')

    # Đơn giản hóa cấu trúc thư mục và tạo thư mục lưu trữ
    output_dir = os.path.join(root_dir, f"data/output/{branch_dir}/{flight_folder_name}")
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Lưu dữ liệu thô vào tệp JSON duy nhất
    output_file_path = os.path.join(output_dir, 'flights_data.json')
    with open(output_file_path, 'w') as json_file:
        json.dump(response, json_file)

    return response



def transform_flights(flights: list) -> list:
    """
        Reshape flights JSON file.

        :param flights: List containing all the flights taken during the trip.
        :return: list containing dictionaries that will be the entries in the larger CSV file.
    """
    # output_filenames = []
    data = []

    for f, flight in enumerate(flights):
        da = flight['departure_airport']
        aa = flight['arrival_airport']

        # Information
        flight_information = dict(
            da_name=da['name'],
            da_id=da['id'],
            da_time=da['time'],
            aa_name=aa['name'],
            aa_id=aa['id'],
            aa_time=aa['time'],
            duration=flight['duration'],
            airplane=flight['airplane'],
            airline=flight['airline'],
            airline_logo=flight['airline_logo'],
            travel_class=flight['travel_class'],
            flight_number=flight['flight_number'],
            # legroom=flight['legroom'],
            additional_information=', '.join(flight['extensions'])
        )

        data.append(flight_information)

    columns = list(flight_information.keys())

    # return columns, data, output_filenames
    return data


def transform_layovers(layovers: list) -> list:
    """
        Reshape layovers JSON file.

        :param layovers: List containing all the information on the layovers information.
        :return: list containing information regarding the layovers.
    """
    # output_filenames = []
    data = []

    for l, lay in enumerate(layovers):
        # Information
        layover_information = dict(
            duration=lay['duration'],
            name=lay['name'],
            id=lay['id']
        )

        data.append(layover_information)

    columns = list(layover_information.keys())

    # return columns, data, output_filenames
    return data


def transform_trips(trips_json: list) -> dict:
    """
        Reshape trips JSON file.

        :param trips_json: List containing trip information.

        :return:
            flight_information : List containing lists, each sublist represents information on a flight.
            trip_information   : List containing lists, each sublist represents information on a trip.
    """
    flight_information = []
    trip_information = []

    for trip in trips_json:
        # Flights
        flights = trip['flights']
        flight_data = transform_flights(flights)
        flight_information.append(flight_data)

        # Trip information
        trip_data = copy.deepcopy(trip)
        del trip_data['flights']  # Xóa flights từ trip data

        trip_information.append(trip_data)

    return flight_information, trip_information


def generate_flight_csv(flights_json: dict, trips_output_dir: str, trip_category: str) -> None:
    """
        Helper function to generate flight CSVs.
        :param flights_json: RAW JSON file containing flight information.
        :param trips_output_dir: Directory where trips information are saved.
        :param trip_category: String in either [`best_flights`, `other_flights`] representing the type of flights.
        :return: No response
    """
    flights_output_dir = os.path.join(trips_output_dir, 'flights')
    trip_info_output_dir = os.path.join(trips_output_dir, 'trip_information')

    if not os.path.exists(trips_output_dir):
        os.makedirs(trips_output_dir)
    if not os.path.exists(flights_output_dir):
        os.makedirs(flights_output_dir)
    if not os.path.exists(trip_info_output_dir):
        os.makedirs(trip_info_output_dir)

    best_trips = flights_json[trip_category]

    flight_information, trip_information = transform_trips(best_trips)

    # Saving flight information
    for i, (flight_info, trip_info) in enumerate(zip(flight_information, trip_information)):
        # Concatenate the multiple flights in a trip, to one dataframe
        flight_filename = f'trip_{i}.csv'
        fl_output_dir = os.path.join(flights_output_dir, flight_filename)

        # Tạo DataFrame từ thông tin chuyến bay
        concatenated_df = pd.DataFrame(flight_info)
        concatenated_df.to_csv(fl_output_dir, index=False)

        # Save trip information
        trip_info_filename = f'info_trip_{i}.csv'
        tinfo_output_dir = os.path.join(trip_info_output_dir, trip_info_filename)

        df = pd.DataFrame(trip_info, columns=list(trip_info.keys()), index=[0])
        df.to_csv(tinfo_output_dir, index=False)



def transform_to_csv(flights_json: dict, silver_output_dir: str) -> None:
    """
        Decouple raw data into multiple CSVs.

        :param flights_json: Raw JSON containing flights information.
        :param silver_output_dir: Directory where to save the silver data.

        :return: No response
    """
    # Search parameters
    search_parameters = flights_json['search_parameters']
    df = pd.DataFrame.from_dict(search_parameters, orient='index')
    search_parameters_output_dir = os.path.join(silver_output_dir, 'search_parameters.csv')
    df.to_csv(search_parameters_output_dir, encoding='utf-8', index=False)

    # Best flights
    print('[i] Generating best trips.')
    best_trips_output_dir = os.path.join(silver_output_dir, 'best_trips')
    generate_flight_csv(flights_json, best_trips_output_dir, 'best_trips')

    # Other flights
    print('[i] Generating other trips.')
    other_trips_output_dir = os.path.join(silver_output_dir, 'other_trips')
    generate_flight_csv(flights_json, other_trips_output_dir, 'other_trips')



if __name__ == '__main__':
    branch_dir = 'HAN-SGN/20241201-20241202/20241024'
    flight_information = {
        "engine": "google_flights",
        "departure_id": "HAN",
        "arrival_id": "SGN",
        "outbound_date": "2024-12-01",
        "return_date": "2024-12-02",
        "currency": "VND",
        "hl": "vi"
    }
    extract_flights_information(branch_dir, flight_information)

