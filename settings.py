# Django settings for MYPROJECT project.

DATABASES = {
    'default': {
    ...
    },

    # Amazon redshift data store.
    'redshift': {
        'ENGINE': 'redshift.db_backend',
        'NAME': 'coverfox',
        'USER': 'coverfox',
        'PASSWORD': 'Coverfox123',
        'HOST': 'logdb.cwbaksyl3qj0.ap-southeast-1.redshift.amazonaws.com',
        'PORT': '5439',
    }
}
