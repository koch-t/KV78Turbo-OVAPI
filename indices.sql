START TRANSACTION;
CREATE INDEX ON localservicegrouppasstime("targetarrivaltime");
CREATE INDEX ON localservicegrouppasstime("targetarrivaltime","localservicelevelcode");
CREATE INDEX ON localservicegrouppasstime(dataownercode,localservicelevelcode,lineplanningnumber,journeynumber,fortifyordernumber);
CREATE INDEX ON localservicegroupvalidity("dataownercode","localservicelevelcode");
COMMIT;
