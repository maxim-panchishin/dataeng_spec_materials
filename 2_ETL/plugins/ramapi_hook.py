from airflow.providers.http.hooks.http import HttpHook


class RAMAPILocationHook(HttpHook):

    def __init__(self, http_conn_id: str = ..., method: str = 'GET', **kwargs):
        """ 
        Initializes the class and takes in a http_conn_id, 
        which is used to identify the connection to the API. 
        The method parameter is set to 'GET' by default.
        """
        super().__init__(http_conn_id=http_conn_id, method=method, **kwargs)


    def __get_location_count(self):
        """
        Helper method that retrieves the number of locations from the API. 
        It sends a GET request to the API endpoint 
        and returns the value of the 'count' key in the JSON response.
        """
        res = self.run('api/location?page=1')

        return res.json()['info']['count']

    
    def gen_location_schema(self):
        """"
        Generator that yields the JSON response 
        for each location in the API. It uses the get_conn method inherited from HttpHook 
        to establish a session with the API. 
        It then uses a for loop to iterate over the range of location IDs 
        provided by __get_location_count.
        """
        session = self.get_conn()
        for location_id in range(1, self.__get_location_count()+1):

            res = session.get(f'{self.base_url}/api/location/{location_id}')
            res.raise_for_status()

            yield res.json()
