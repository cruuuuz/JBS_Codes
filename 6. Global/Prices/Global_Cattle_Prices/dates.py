import datetime

class Dates:
    @staticmethod
    def dates(): 
        all_dates = {}
        
        now = datetime.datetime.now()

        all_dates.update({'day': now.day})
        all_dates.update({'month': now.month})
        all_dates.update({'year': now.year})

        week = int(datetime.date(all_dates["year"], all_dates["month"], all_dates["day"]).strftime("%V"))
        week = week - 1

        all_dates.update({'week': week})

        return all_dates
