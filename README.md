# django-redshift
Redshift ORM and Migration support for django. This app provides support for accessing AWS Redshift database using django ORM. It also contains a small migration tool to manage your migrations.

## Getting Started:

**Connect to Redshift instance**
Add your redshfit config to django settings as eg below.
```python
DATABASES = {
    'default': {
    ...
    },

    # Amazon redshift data store.
    'redshift': {
        'ENGINE': 'redshift.db_backend',
        'NAME': 'database_name',
        'USER': 'db_user',
        'PASSWORD': 'Password@123',
        'HOST': 'logdb.cwbaksyl3qj0.ap-southeast-1.redshift.amazonaws.com',
        'PORT': '5439',
    }
}
```

**Create your model inherting from `RedshiftModel` class**
```python
class Nginx(RedshiftModel):
    visitor_id = models.CharField(max_length=32, null=True)
    user_id = models.IntegerField(null=True)
    process_name = models.CharField(max_length=25)
    ip = models.GenericIPAddressField(null=True)
    request_type = models.CharField(max_length=10)
    http_status = models.IntegerField()
    url = models.CharField(max_length=3000, null=True)
    event_at = models.DateTimeField()
    adposition = models.CharField(max_length=25, null=True)
    device_type = models.CharField(max_length=25, null=True)
    created_at = models.DateTimeField(auto_now_add=True)

    def __unicode__(self):
        return '%s - %s' % (self.id, self.url)

    class Meta:
        db_table = "nginx"
   ```
   
   **Create and Run migrations**
   Documented here https://github.com/HemantNegi/django-redshift/blob/master/migrations/README.txt
   
   Use this model as a normal django model. It supports most of the ORM operations.
   
   
