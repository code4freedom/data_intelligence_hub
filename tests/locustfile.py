from locust import HttpUser, task, between

class RVToolsUser(HttpUser):
    wait_time = between(1, 3)

    @task(2)
    def upload_manifest(self):
        # simulate checking manifests
        self.client.get('/manifests')

    @task(1)
    def create_job(self):
        # try to create a job for a known manifest
        self.client.post('/jobs/create', data={'manifest_name': 'manifest_vInfo_localtest.json', 'template':'vsphere'})
