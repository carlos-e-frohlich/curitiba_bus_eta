import datetime as dt
import pytz

# Function: resolve_update_datetime


def resolve_update_datetime(request_datetime, update_time):
    '''
    Return the resolved update datetime, correcting for
    inconsistencies more likely to occur around midnight.

    Arguments:
        request_datetime -- The timezone-aware request datetime
        update_time -- The timezone-aware update time
    '''

    update_datetime = dt.datetime.combine(request_datetime.date(), update_time)

    update_datetime = pytz.timezone('America/Sao_Paulo').localize(update_datetime)

    if update_datetime > request_datetime:
        update_datetime -= dt.timedelta(days=1)
    else:
        pass

    return update_datetime
