from django.conf.urls import url
from django.contrib import admin
from imdbcrawler import views

urlpatterns = [
	url(r'^admin/', admin.site.urls),
	url(r'^$', views.index, name="index"),
	url(r'^import/', views.importData, name="importData"),
	url(r'^parse/', views.parse, name="parse"),
]
