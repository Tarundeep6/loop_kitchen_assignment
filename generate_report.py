import os
import csv
import pandas as pd
from pytz import timezone
from datetime import datetime, timedelta


def getDateTime(timestamp_utc):
    l = timestamp_utc.split(" ")
    l1 = l[1].split(".")
    return datetime.strptime(l[0] + " " + l1[0], "%Y-%m-%d %H:%M:%S")


def getDateTimeNew(timestamp_utc):
    l = timestamp_utc.split(" ")
    return datetime.strptime(l[0] + " " + l[1], "%Y-%m-%d %H:%M:%S")


def is_timestamp_in_range(curr_time_stamp, start_timestamp, end_timestamp):
    return curr_time_stamp >= start_timestamp and curr_time_stamp <= end_timestamp


def is_active(status):
    return "active" == status


class generate_report_class:
    export_path = None

    def __init__(self, store_status_path, menu_hours_path, timezone_map, export_path):
        store_status_file = open(store_status_path)
        menu_hours_file = open(menu_hours_path)
        timezone_file = open(timezone_map)
        self.export_path = export_path

        store_status_list = csv.reader(store_status_file)
        self.store_status_list_map = {}
        is_first_line = True
        for store_entry in store_status_list:
            if is_first_line:
                is_first_line = False
            else:
                store_id = store_entry[0]
                store_status_list_map_entry = self.store_status_list_map.get(
                    store_id, []
                )
                datetime_obj = getDateTime(store_entry[2])
                store_status_list_map_entry.append([store_entry[1], datetime_obj])
                self.store_status_list_map[store_id] = store_status_list_map_entry
        print(len(self.store_status_list_map))

        timezone_list = csv.reader(timezone_file)
        self.timezone_map = {}
        is_first_line = True
        for timezone in timezone_list:
            # if timezone[1] == "":
            # 	self.timezone_map[timezone[0]] = "America/Chicago"
            # else:
            if is_first_line:
                is_first_line = False
            else:
                self.timezone_map[timezone[0]] = timezone[1]
        print(len(self.timezone_map))

        menu_hours_list = csv.reader(menu_hours_file)
        self.menu_hours_map = {}
        is_first_line = True
        for menu in menu_hours_list:
            if is_first_line:
                is_first_line = False
            else:
                menu_hours_list_entry = self.menu_hours_map.get(menu[0], [])
                menu_hours_list_entry.append([menu[1], menu[2], menu[3]])
                self.menu_hours_map[menu[0]] = menu_hours_list_entry

        print(len(self.menu_hours_map))

    def generate_detailed_report(self, curr_time, report_id):
        curr_time = datetime.strptime("2023-01-24 13:06:07", "%Y-%m-%d %H:%M:%S")
        all_report = []
        for store_id in self.store_status_list_map.keys():
            last_hour = curr_time - timedelta(hours=1)
            last_day = curr_time - timedelta(days=1)
            last_week = curr_time - timedelta(weeks=1)
            uptime_last_hour, downtime_last_hour = self.generate_report(
                store_id, last_hour, curr_time
            )
            uptime_last_day, downtime_last_day = self.generate_report(
                store_id, last_day, curr_time
            )
            uptime_last_week, downtime_last_week = self.generate_report(
                store_id, last_week, curr_time
            )
            report = {
                "store_id": store_id,
                "uptime_last_hour": uptime_last_hour,
                "uptime_last_day": uptime_last_day,
                "uptime_last_week": uptime_last_week,
                "downtime_last_hour": downtime_last_hour,
                "downtime_last_day": downtime_last_day,
                "downtime_last_week": downtime_last_week,
            }
            all_report.append(report)

        dump_path = os.path.join(self.export_path, report_id + ".csv")
        df = pd.DataFrame(all_report)
        df.to_csv(dump_path)
        return dump_path

    def generate_report(self, store_id, start_timestamp, end_timestamp):
        active_count = 0
        inactive_count = 0
        # print("new time", getDateTimeNew(start_timestamp))
        for store_status in self.store_status_list_map.get(store_id):
            # Need to modify the active and inactive logic
            curr_time_stamp = store_status[1]
            # curr_time_stamp = datetime.timestamp(store_status[1])
            # print("curr time", curr_time_stamp)
            if is_timestamp_in_range(
                curr_time_stamp, start_timestamp, end_timestamp
            ) and self.is_timestamp_in_bussiness_hours(
                store_status[1], store_status[0]
            ):
                if is_active(store_status[0]):
                    active_count += 1
                else:
                    inactive_count += 1
        return [active_count, inactive_count]

    def get_time_of_day(self, datetime_, store_id):
        timezone_val = self.timezone_map.get(store_id, "America/Chicago")
        datetime_local = datetime_.astimezone(timezone(timezone_val))
        return datetime_local.strftime("%H:%M:%S")

    def is_timestamp_in_bussiness_hours(self, curr_date_time, store_id):
        curr_time_stamp = self.get_time_of_day(curr_date_time, store_id)
        day = curr_date_time.weekday()
        menu_hours = self.menu_hours_map.get(store_id, [])
        if menu_hours == []:
            return True
        for menu in menu_hours:
            if menu[0] == day and is_timestamp_in_range(
                curr_time_stamp, menu[1], menu[2]
            ):
                return True
        return False
