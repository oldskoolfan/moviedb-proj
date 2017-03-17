"""
url patterns for web interface
"""

from django.conf.urls import url
from django.contrib import admin
from imdbcrawler.views import *

urlpatterns = [
    url(r'^admin/', admin.site.urls),
    url(r'^$', IndexView.as_view(), name="index"),
]
