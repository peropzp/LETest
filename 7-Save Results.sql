set @@dataset_project_id = 'letest-409019';
set @@dataset_id = 'LETestDataset';

insert into DistanceFuel 
select date, 
        MachineId, 
        startDateTime, 
        stopDateTime,  
        RunPointStart, 
        RunPointEnd, 
        st_length(multiline), 
        FuelConsumed
from multilines;

insert into Coverage
select 
    case when (select max(runID) from Coverage) is null then ROW_NUMBER() OVER()
    else ROW_NUMBER() OVER() + (select max(runID) from Coverage)
    end, startDateTime, stopDatetime, polygon
from multilines;

