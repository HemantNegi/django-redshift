# __author__ = 'Hemant Negi <hemant.frnz@gmail.com>'

from django.db import models


class CustomDBManager(models.Manager):

    db = None

    def __init__(self, db, *args, **kwargs):
        self.db = db
        super(CustomDBManager, self).__init__(*args, **kwargs)

    def get_queryset(self):
         return super(CustomDBManager, self).get_queryset().using(self.db)


class RedshiftModel(models.Model):
    """
    Base model to support database operations on redshift
    """

    objects = CustomDBManager(db='redshift')

    def save(self, **kwargs):
        _kwargs = {'using': 'redshift'}
        _kwargs.update(kwargs)
        super(RedshiftModel, self).save(**_kwargs)

    class Meta:
        abstract = True


class test_data(RedshiftModel):

    username = models.CharField(max_length=25)
    age = models.IntegerField()

    def __unicode__(self):
        return '%s - %s' % (self.id, self.username)

    class Meta:
        db_table = "test_data"


class Activity(RedshiftModel):
    """
    single row insert
    a = Activity(activity_type='test_Activity', server_id="1", user_id="12", app="custom", data="{}")
    a.save()

    BULK INSERT
    Activity.objects.bulk_create([
        Activity(activity_type='test_Activity', server_id="1", user_id="13", app="custom", data="{}"),
        Activity(activity_type='test_Activity', server_id="1", user_id="14", app="custom", data="{}")
    ])
    """

    activity_type = models.CharField(max_length=30)
    server_id = models.CharField(max_length=50)
    user_id = models.IntegerField(null=True)  # foreign key to users
    person_id = models.CharField(max_length=36, null=True)
    request_id = models.CharField(max_length=36, null=True)
    app = models.CharField(max_length=30)
    user_agent = models.CharField(max_length=100, null=True)
    data1 = models.CharField(max_length=500, null=True)
    data2 = models.CharField(max_length=500, null=True)
    created_at = models.DateTimeField(auto_now_add=True)
    data = models.CharField(max_length=2000)

    def __unicode__(self):
        return '%s - %s' % (self.id, self.activity_type)

    class Meta:
        db_table = "activity"


class Nginx(RedshiftModel):

    # request_id       | character varying(36)       | not null (converted to id as primary key)
    visitor_id = models.CharField(max_length=32, null=True)
    user_id = models.IntegerField(null=True)
    process_name = models.CharField(max_length=25)
    ip = models.GenericIPAddressField(null=True)
    request_type = models.CharField(max_length=10)
    http_status = models.IntegerField()
    url = models.CharField(max_length=3000, null=True)
    redirection_url = models.CharField(max_length=3000, null=True)
    event_at = models.DateTimeField()
    adposition = models.CharField(max_length=25, null=True)
    device_type = models.CharField(max_length=25, null=True)
    device_model = models.CharField(max_length=50, null=True)
    network = models.CharField(max_length=50, null=True)
    network_category = models.CharField(max_length=50, null=True)
    utm_campaign = models.CharField(max_length=32, null=True)
    utm_medium = models.CharField(max_length=50, null=True)
    utm_source = models.CharField(max_length=50, null=True)
    utm_term = models.CharField(max_length=50, null=True)
    keyword = models.CharField(max_length=50, null=True)
    gclid = models.CharField(max_length=100, null=True)
    creative = models.CharField(max_length=50, null=True)
    source = models.CharField(max_length=50, null=True)
    utm_content = models.CharField(max_length=50, null=True)
    created_at = models.DateTimeField(auto_now_add=True)

    def __unicode__(self):
        return '%s - %s' % (self.id, self.url)

    class Meta:
        db_table = "nginx"