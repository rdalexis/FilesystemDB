DROP PROCEDURE IF EXISTS grep;
DELIMITER @@

CREATE PROCEDURE grep(
    IN file_id BIGINT,
    IN search_string VARCHAR(255)
)
BEGIN
    DECLARE NumOfLines INT DEFAULT 0;
    DECLARE IterationCount INT DEFAULT 0;
    DECLARE ExtractedData LONGBLOB;
    DECLARE CurrentLine VARCHAR(255) DEFAULT '';
    DECLARE StringPosition INT DEFAULT 0;
    DECLARE TempData LONGBLOB;

    SELECT ROUND((length(data)-length(replace(data, "\n", "")))/length("\n")) INTO NumOfLines 
    FROM tree AS t INNER JOIN fdata AS f 
    WHERE t.fid = file_id AND t.nodeid = f.nodeid;

    SELECT data INTO TempData FROM tree AS t INNER JOIN fdata AS f WHERE t.fid = file_id AND t.nodeid = f.nodeid;
    WHILE IterationCount <= NumOfLines DO
        SELECT SUBSTRING_INDEX(TempData, "\n", IterationCount) INTO ExtractedData;
        SELECT SUBSTRING_INDEX(ExtractedData, "\n", -1) INTO CurrentLine;
        SELECT POSITION(search_string IN CurrentLine) INTO StringPosition;
        IF StringPosition > 0 THEN
           SELECT IterationCount AS LineNumber, CurrentLine;
        END IF;
        SET IterationCount = IterationCount + 1;
        SET CurrentLine = '';
        SET StringPosition = 0;
    END WHILE;
END@@
 
DELIMITER ;
