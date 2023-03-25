"""
custom operator named RAMAPILocationOperator which is used to retrieve data 
from the RAM API and find the top 3 locations by the number of residents.
"""


from airflow.models import BaseOperator
from plugins.ramapi_hook import RAMAPILocationHook


class RAMAPILocationOperator(BaseOperator):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)


    def execute(self, **kwargs):
        """
        Called when the task is run and retrieves data from the RAM API using the RAMAPILocationHook. 
        The retrieved data is then processed to find the top 3 locations by the number of residents. 
        The top 3 locations are returned as a string.
        """
        
        # instantiate a hook to interact with the RAM API
        hook = RAMAPILocationHook('dina_ram')
        locations = []

        # iterate over location schemas generated by the hook
        for location in hook.gen_location_schema():

            # create a tuple with location data
            row = (
                location['id'],
                location['name'],
                location['type'],
                location['dimension'],
                len(location['residents'])
            )
            self.log.info(tuple(row))
            locations.append(row)

        # find the top 3 locations by the number of residents
        top3_locations = sorted(locations, key=lambda x: x[-1], reverse=True)[:3]
        # join the top 3 locations into a string
        return_value = ','.join(map(str, top3_locations))
        self.log.info('Found top-3:')
        self.log.info(return_value)

        # return the top 3 locations as a string
        return return_value