#geocoding

download.file(paste0('http://www.naturalearthdata.com/http//',
                     'www.naturalearthdata.com/download/110m/cultural/',
                     'ne_110m_admin_0_countries.zip'), 
              f <- tempfile())
unzip(f, exdir=tempdir())
library(rgdal)
countries <- readOGR(tempdir(), 'ne_110m_admin_0_countries')

pts <- data.frame(x=runif(10, -180, 180), y=runif(10, -90, 90),
                  VAR1=LETTERS[1:10])


existingGeocodedAddress <- readCsvFromDirectory('geocodedAddress', parameters$path_geocoded_address)
table(is.na(existingGeocodedAddress$longitude), useNA = 'ifany')
table(is.na(existingGeocodedAddress$latitude), useNA = 'ifany')
table(existingGeocodedAddress$location_type)

pts <- data.frame(existingGeocodedAddress[, .(x = as.double(longitude), y = as.double(latitude))])
coordinates(pts) <- ~x+y  # pts needs to be a data.frame for this to work
proj4string(pts) <- proj4string(countries)

plot(countries)
points(pts, pch=20, col='red')


existingGeocodedAddress[, .(x = longitude, y = latitude)]
