import warnings

warnings.simplefilter(action="ignore", category=FutureWarning)
import json
from multiprocessing import Pool
import json
import logging
from urllib.request import urlopen
from csv import writer

import pandas as pd
from tqdm.contrib.concurrent import process_map


base_bike_url = "https://iot.hamburg.de/v1.1/Things?$skip=0&$top=5000&$filter=((properties%2FownerThing+eq+%27DB+Connect%27))"
start_date = "2022-05-01"
end_date = "2022-09-30"


def get_stations():
    """
    Get all stations
    """
    response = json.loads(urlopen(base_bike_url).read())
    return response["value"]


def get_station_urls(station):
    """
    Get all urls of a station
    """
    try:
        thingID = station["@iot.id"]
        datastream_url = station["Datastreams@iot.navigationLink"]
        description = station["description"]
    except ValueError:
        logging.error("Not valid value found.")

    return {
        "thingID": thingID,
        "datastream_url": datastream_url,
        "description": description,
    }


def get_obs_stream(station):
    """
    Get the observation stream of station
    """
    try:
        response = json.loads(urlopen(station["datastream_url"]).read())["value"]
        keep_datastream = response[0]
        obs_stream = keep_datastream["Observations@iot.navigationLink"]

        coordinatesAll = keep_datastream["observedArea"]["coordinates"]

        coordinatesX = "NA"
        coordinatesY = "NA"

        def get_correct_vals(item):
            if any(isinstance(subitem, list) for subitem in item):

                for subitem in item:
                    if any(isinstance(subsubitem, list) for subsubitem in subitem):
                        return get_correct_vals(subitem)

                    if len(subitem) == 2:
                        # try to locate correct coordinate values (order seems to be random)
                        if (
                            subitem[0] > 0
                            and subitem[0] <= 180
                            and subitem[1] > 0
                            and subitem[1] <= 180
                        ):
                            return subitem[0], subitem[1]

            else:
                return item[0], item[1]

        coordinatesX, coordinatesY = get_correct_vals(coordinatesAll)

        return {
            "thingID": station["thingID"],
            "description": station["description"],
            "obs_stream": obs_stream,
            "coordinatesX": coordinatesX,
            "coordinatesY": coordinatesY,
        }

    except KeyError:
        logging.error(
            f"No proper observation stream for ID {station['thingID']} found."
        )


def get_obs(station, start_date, end_date):
    """
    Get actual observations for station between to dates. Will iterate over all entries as url only provides 5000 obs per time
    """
    obs_url = (
        station["obs_stream"]
        + f"?$top=5000&$skip={{}}&$filter=date(phenomenontime)+ge+date({start_date})+and+date(phenomenontime)+le+date({end_date})"
    )
    list_of_obs = []
    # as we don't know the amount of results we just iterate until there are no more results
    for i in range(0, 10000000, 5000):
        obs_iter = json.loads(urlopen(obs_url.format(i)).read())["value"]
        if len(obs_iter) == 0:
            break
        list_of_obs = list_of_obs + obs_iter

    return list_of_obs


def export_obs_for_station(station):
    """
    Export observations into export file.
    Also exports meta data into meta data file.

    """

    list_of_cleaned_obs = []
    station_stream = get_station_urls(station)
    obs_stream = get_obs_stream(station_stream)
    all_obs = get_obs(obs_stream, start_date, end_date)

    meta_dict = {
        key: obs_stream[key]
        for key in ("thingID", "description", "coordinatesX", "coordinatesY")
    }

    for obs in all_obs:
        list_of_cleaned_obs.append(
            [meta_dict["thingID"], obs["resultTime"], obs["result"]]
        )

    df = pd.DataFrame(
        columns=["thingID", "resultTime", "result"], data=list_of_cleaned_obs
    )
    df["resultTime"] = pd.to_datetime(df["resultTime"])
    df = df.sort_values("resultTime")

    # get last entry per hour for missing entries

    df_last = df.groupby(df["resultTime"].dt.floor("h")).last("result").reset_index()
    df["resultTime"] = df["resultTime"].dt.floor("h")
    df_mean = df.groupby("resultTime").mean("result").reset_index()
    df_mean = df_mean.rename({"result": "resultAverage"}, axis=1)

    all_hours = pd.DataFrame(
        pd.date_range(
            pd.to_datetime(start_date + " " + "00:00"),
            pd.to_datetime(end_date + " " + "23:00"),
            freq="H",
        ),
        columns=["resultTime"],
        dtype="datetime64[ns, UTC]",
    )

    all_hours_df = pd.merge(all_hours, df_last, on="resultTime", how="left")
    if len(df_last) == 0:
        print(f"no entries: {meta_dict['thingID']}")
        all_hours_df["resultAverage"] = pd.NA

    else:
        all_hours_df["result"] = all_hours_df["result"].fillna(method="ffill")

        all_hours_df = pd.merge(
            all_hours_df,
            df_mean[["resultTime", "resultAverage"]],
            on="resultTime",
            how="left",
        )
        all_hours_df["resultAverage"] = all_hours_df["resultAverage"].combine_first(
            all_hours_df["result"]
        )
    all_hours_df["thingID"] = meta_dict["thingID"]
    export_obs(all_hours_df)
    export_meta(meta_dict)


def export_meta(meta_dict):
    with open("results/meta_data.csv", "a") as event_f:
        writer_object = writer(event_f)
        writer_object.writerow(
            [
                meta_dict["thingID"],
                meta_dict["description"],
                meta_dict["coordinatesX"],
                meta_dict["coordinatesY"],
            ]
        )


def export_obs(obs_df):
    obs_df[["thingID", "resultTime", "resultAverage"]].to_csv(
        "results/event.csv", mode="a", index=False, header=False
    )


def run():
    all_stations = get_stations()

    with open("results/event.csv", "w") as event_f:
        writer_object = writer(event_f)
        writer_object.writerow(["stationID", "resultTime", "resultAverage"])
    with open("results/meta_data.csv", "w") as event_f:
        writer_object = writer(event_f)
        writer_object.writerow(
            ["thingID", "description", "coordinatesX", "coordinatesY"]
        )

    # for station in all_stations:
    #     export_obs_for_station(station)
    with Pool() as p:
        res = process_map(export_obs_for_station, all_stations)


if __name__ == "__main__":
    run()
