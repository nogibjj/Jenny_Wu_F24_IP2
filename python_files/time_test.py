from datetime import datetime
from query_data import query_read

start = datetime.now()
query_read(database="nypd_shooting.db", table="nypd_shooting")
end = datetime.now()

start_mirco = start.microsecond
end_mirco = end.microsecond

total_time = end_mirco - start_mirco
print(
    "The start time was {} and the end time was {}.".format(start, end),
    "The Python Speed test took: {} microseconds to complete.".format(total_time),
)
