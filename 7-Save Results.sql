set @@dataset_project_id = 'letest-409019';
set @@dataset_id = 'LETestDataset';

insert into DistanceFuel 
select date, MachineId, startDateTime, stopDateTime,  
    st_geogpoint(startLongitude, startLatitude) as RunPointStart, 
    st_geogpoint(stopLongitude, stopLatitude) as RunPointEnd, 
    st_length(st_geogfromtext(multiline,make_valid => TRUE)) as len, 
    FuelConsumed
from multilines;

insert into Coverage
select 0, startDateTime, stopDatetime, st_boundary(multiline)
from multilines;

update Coverage set RunId = row_number() Over() where runId=0;
