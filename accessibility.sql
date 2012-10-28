drop table accessibility;

CREATE TABLE "accessibility" (
   "timingpointcode" VARCHAR(10)   NOT NULL,
   "timingpointname" VARCHAR(50)   NOT NULL,
   "wheelchairaccessible" VARCHAR(13) NOT NULL,
   "visualaccessible" VARCHAR(13) NOT NULL,
   PRIMARY KEY ("timingpointcode")
);
copy accessibility from '/tmp/accessibility.tsv' delimiter '|' CSV;
alter table timingpoint add column wheelchairaccessible VARCHAR(13);
alter table timingpoint add column visualaccessible VARCHAR(13);

update timingpoint set wheelchairaccessible = 'ACCESSIBLE' WHERE timingpointcode in 
(SELECT timingpointcode FROM accessibility WHERE wheelchairaccessible = 'ACCESSIBLE');

update timingpoint set wheelchairaccessible = 'NOTACCESSIBLE' WHERE timingpointcode in 
(SELECT timingpointcode FROM accessibility WHERE wheelchairaccessible = 'NOTACCESSIBLE');

update timingpoint set visualaccessible = 'ACCESSIBLE' WHERE timingpointcode in 
(SELECT timingpointcode FROM accessibility WHERE visualaccessible = 'ACCESSIBLE');

update timingpoint set visualaccessible = 'NOTACCESSIBLE' WHERE timingpointcode in 
(SELECT timingpointcode FROM accessibility WHERE visualaccessible = 'NOTACCESSIBLE');
