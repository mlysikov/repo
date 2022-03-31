-- Foreword:
-- After researching ways to convert text from one encoding to another, I came across Tom Kytes's post: 
-- https://asktom.oracle.com/pls/apex/f?p=100:11:0::::P11_QUESTION_ID:9538292200346835762
-- which inspired me to write a complete solution that fits perfectly to my task.

-- Oracle version:
-- Oracle Database 19c Enterprise Edition Release 19.0.0.0.0 - Production

-- Run code as user SYS.
-- Create a directory.
create or replace directory output_dir as '/u01/userhome/oracle/output';
-- Grant privileges to the directory to SCOTT.
grant read, write on directory output_dir to scott;

-- Grant privileges to call the SYS packages to SCOTT.
grant execute on sys.utl_file to scott;
grant execute on sys.utl_i18n to scott;

-- Run as user SCOTT.
-- Data preparation.
create table tab
(
  first_name varchar2(100)
 ,last_name  varchar2(100)
 ,birth_date date
);

insert into tab values ('Иван', 'Иванов', to_date('1980-01-01', 'yyyy-mm-dd'));
insert into tab values ('Петр', 'Петров', to_date('1980-01-01', 'yyyy-mm-dd'));

commit;

-- Create a procedure.
create or replace procedure convert_from_utf8_to_win1251
is
  l_file_name     constant varchar2(100) := 'converted_file.csv';
  c_max_line_size constant number := 32767;
  c_crlf          constant varchar2(8) := chr(13) || chr(10);
  l_file                   utl_file.file_type;

  function change_encoding(p_text varchar2)
  return raw
  is
    l_result raw(2000);
  begin
    if ( trim(p_text) is not null )
    then
      l_result := sys.utl_i18n.string_to_raw(p_text, 'CL8MSWIN1251');
    end if;

    return l_result;
  end;
begin
  l_file := sys.utl_file.fopen(location     => 'OUTPUT_DIR'
                              ,filename     => l_file_name
                              ,open_mode    => 'wb' -- write byte mode
                              ,max_linesize => c_max_line_size);
  
  sys.utl_file.put_raw(file   => l_file
                      ,buffer => change_encoding('FIRST_NAME' || ',' ||
                                                 'LAST_NAME'  || ',' ||
                                                 'BIRTH_DATE' || c_crlf));

  for rec in (select t.*
              from   tab t)
  loop
    sys.utl_file.put_raw(file   => l_file
                        ,buffer => change_encoding(rec.first_name || ',' ||
                                                   rec.last_name  || ',' ||
                                                   to_char(rec.birth_date, 'yyyy-mm-dd') || c_crlf));
  end loop;

  sys.utl_file.fclose(file => l_file);

  exception
    when others then
      sys.utl_file.fclose(file => l_file);
      raise;
end;
/

-- Run the procedure.
begin
  convert_from_utf8_to_win1251;
end;
/

-- Clean up.
drop procedure convert_from_utf8_to_win1251;
drop table tab purge;

-- Afterword:
-- Nevertheless, in Oracle Linux, such a command:
-- [oracle@localhost output]$ file -bi converted_file.csv
-- shows:
-- text/plain; charset=iso-8859-1
-- But when I copy the file to Windows and open it with Notepad++, the actual encoding is Windows-1251.
