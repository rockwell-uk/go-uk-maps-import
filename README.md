# Go UK Maps Importer
Imports Ordnance Survey VectorMapDistrict Open Source Data from the Ordnance Survey Data Hub
```
https://api.os.uk/downloads/v1/products/VectorMapDistrict/downloads
```

### Usage
```
go build go-uk-maps-import.go
./go-uk-maps-import -h
```

### Examples
```
./go-uk-maps-import -v -dbengine mysql -dbport 3307 -download
./go-uk-maps-import -v -dbengine mysql -dbport 3307 -countsonly
```

### OSData Copyright
All osdata is copyright © Crown: https://www.ordnancesurvey.co.uk/business-government/licensing-agreements/copyright-acknowledgements
* Contains OS data © Crown copyright [and database right] [year].
* Cynnwys data OS Ⓗ Hawlfraint y Goron [a hawliau cronfa ddata] OS [flwyddyn].

## Author
This software was engineered by [David Boyle](https://github.com/dbx123) @ [Rockwell Consultants Ltd.](https://www.rockwellconsultants.co.uk/)
admin@rockwellconsultants.co.uk / david@davidboyle.co.uk

### Credits
Special thanks to [Chengze Yu](https://github.com/yuchengze) for help and moral support along the way