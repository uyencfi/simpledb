package embedded;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.sql.Statement;

import simpledb.jdbc.embedded.EmbeddedDriver;

public class CreateStudentDB {
   public static void main(String[] args) {
      Driver d = new EmbeddedDriver();
      String url = "jdbc:simpledb:studentdb";

      try (Connection conn = d.connect(url, null);
            Statement stmt = conn.createStatement()) {
         String s = "create table STUDENT(SId int, SName varchar(16), MajorId int, GradYear int)";
         stmt.executeUpdate(s);
         System.out.println("Table STUDENT created.");

         s = "create index idxMajor on student(majorid) using hash";
         stmt.executeUpdate(s);
         System.out.println("index idxMajor created on student(majorid) hash");

         s = "insert into STUDENT(SId, SName, MajorId, GradYear) values ";
         String[] studvals = {
                 "(1, 'joe', 10, 2021)",
                 "(2, 'amy', 20, 2020)",
                 "(3, 'max', 10, 2022)",
                 "(4, 'sue', 20, 2022)",
                 "(5, 'bob', 30, 2020)",
                 "(6, 'kim', 20, 2020)",
                 "(7, 'art', 30, 2021)",
                 "(8, 'pat', 20, 2019)",
                 "(9, 'lee', 10, 2021)",
                 "(10, 'leean', 40, 2021)",
                 "(11, 'joey', 40, 2021)",
                 "(12, 'amie', 50, 2020)",
                 "(13, 'maxim', 50, 2022)",
                 "(14, 'sueann', 60, 2022)",
                 "(15, 'bobie', 60, 2018)",
                 "(16, 'kimi', 70, 2020)",
                 "(17, 'artichoke', 70, 2021)",
                 "(18, 'pattie', 80, 2019)",
                 "(19, 'lee jong suk', 80, 2021)",
                 "(20, 'lee sim', 90, 2021)",
                 "(21, 'joanne', 90, 2021)",
                 "(22, 'amanda', 100, 2020)",
                 "(23, 'maxie', 100, 2022)",
                 "(24, 'suey', 110, 2022)",
                 "(25, 'bobber', 110, 2020)",
                 "(26, 'kimly', 120, 2018)",
                 "(27, 'arthur', 120, 2021)",
                 "(28, 'patrina', 130, 2019)",
                 "(29, 'jodi', 130, 2021)",
                 "(30, 'uyen', 140, 2021)",
                 "(31, 'shee hui', 140, 2021)",
                 "(32, 'claire', 150, 2020)",
                 "(33, 'mason', 150, 2022)",
                 "(34, 'susan', 160, 2022)",
                 "(35, 'bob ross', 160, 2020)",
                 "(36, 'atica', 170, 2018)",
                 "(37, 'erica', 170, 2021)",
                 "(38, 'patrick', 180, 2019)",
                 "(39, 'cat', 180, 2021)",
                 "(40, 'nicholas', 190, 2021)",
                 "(41, 'pricilla', 190, 2021)",
                 "(42, 'gerald', 200, 2020)",
                 "(43, 'ben', 200, 2022)",
                 "(44, 'lucas', 210, 2022)",
                 "(45, 'fabrianne', 210, 2020)",
                 "(46, 'warren buffet', 220, 2018)",
                 "(47, 'bharath', 220, 2021)",
                 "(48, 'david', 230, 2019)",
                 "(49, 'rushil', 230, 2021)",
                 "(50, 'dean', 240, 2020)"
         };
         for (int i=0; i<studvals.length; i++)
            stmt.executeUpdate(s + studvals[i]);
         System.out.println("STUDENT records inserted.");

         s = "create table DEPT(DId int, DName varchar(20))";
         stmt.executeUpdate(s);
         System.out.println("Table DEPT created.");

         s = "insert into DEPT(DId, DName) values ";
         String[] deptvals = {
                 "(10, 'compsci')",
                 "(20, 'math')",
                 "(30, 'drama')",
                 "(40, 'english')",
                 "(50, 'literature')",
                 "(60, 'economics')",
                 "(70, 'history')",
                 "(80, 'german studies')",
                 "(90, 'japanese studies')",
                 "(100, 'european studies')",
                 "(110, 'american studies')",
                 "(120, 'asian studies')",
                 "(130, 'social work')",
                 "(140, 'psychology')",
                 "(150, 'political science')",
                 "(160, 'finance')",
                 "(170, 'stats')",
                 "(180, 'chem')",
                 "(190, 'physics')",
                 "(200, 'bio')",
                 "(210, 'biomed')",
                 "(220, 'medicine')",
                 "(230, 'dentistry')",
                 "(240, 'pharmacy')",
                 "(250, 'mech eng')",
                 "(260, 'chem eng')",
                 "(270, 'biomed eng')",
                 "(280, 'electrical eng')",
                 "(290, 'food tech')",
                 "(300, 'material science')",
                 "(310, 'law')",
                 "(320, 'french')",
                 "(330, 'german')",
                 "(340, 'bza')",
                 "(350, 'information systems')",
                 "(360, 'cybersecurity')",
                 "(370, 'cnm')",
                 "(380, 'ppe')",
                 "(390, 'veterinary')",
                 "(400, 'philosophy')",
                 "(410, 'industrial design')",
                 "(420, 'music')",
                 "(430, 'sociology')",
                 "(440, 'geography')",
                 "(450, 'data science')",
                 "(460, 'management')",
                 "(470, 'life sciences')",
                 "(480, 'chinese studies')",
                 "(490, 'nursing')",
                 "(500, 'accountancy')"
         };

         for (int i=0; i<deptvals.length; i++)
            stmt.executeUpdate(s + deptvals[i]);
         System.out.println("DEPT records inserted.");

         s = "create table COURSE(CId int, Title varchar(20), DeptId int)";
         stmt.executeUpdate(s);
         System.out.println("Table COURSE created.");

         s = "insert into COURSE(CId, Title, DeptId) values ";
         String[] coursevals = {
                 "(12, 'db systems', 10)",
                 "(22, 'compilers', 10)",
                 "(32, 'calculus', 20)",
                 "(42, 'algebra', 20)",
                 "(52, 'acting', 30)",
                 "(62, 'elocution', 30)",
                 "(72, 'diction', 40)",
                 "(82, 'old english', 40)",
                 "(92, 'politics', 50)",
                 "(102, 'shakespeare', 50)",
                 "(112, 'price model', 60)",
                 "(122, 'competitiveness', 60)",
                 "(132, 'ww2', 70)",
                 "(142, 'ww3', 70)",
                 "(152, 'hitler', 80)",
                 "(162, 'stalin', 80)",
                 "(172, 'royals', 90)",
                 "(182, 'war', 90)",
                 "(192, 'EU', 100)",
                 "(202, 'obama', 110)",
                 "(212, 'economic boom', 120)",
                 "(222, 'culture', 120)",
                 "(232, 'child abuse', 130)",
                 "(242, 'poverty', 130)",
                 "(252, 'brain', 140)",
                 "(262, 'cognitive science', 140)",
                 "(272, 'american jury', 150)",
                 "(282, 'intro to politics', 150)",
                 "(292, 'tax', 160)",
                 "(302, 'linear algebra', 170)",
                 "(312, 'probability', 170)",
                 "(322, 'organic', 180)",
                 "(332, 'alcohols', 180)",
                 "(342, 'astrophysics', 190)",
                 "(352, 'reproduction', 200)",
                 "(362, 'vaccines', 210)",
                 "(372, 'eczema', 220)",
                 "(382, 'surgical', 220)",
                 "(392, 'oral', 230)",
                 "(402, 'toothpaste', 230)",
                 "(412, 'tablets', 240)",
                 "(422, 'chemistry', 240)",
                 "(432, 'systems', 250)",
                 "(442, 'python', 250)",
                 "(452, 'chem', 260)",
                 "(462, 'vaccine', 270)",
                 "(472, 'chem', 270)",
                 "(482, 'java', 280)",
                 "(492, 'resistors', 280)",
                 "(502, 'msg', 290)"
         };
         for (int i=0; i<coursevals.length; i++)
            stmt.executeUpdate(s + coursevals[i]);
         System.out.println("COURSE records inserted.");

         s = "create table SECTION(SectId int, CourseId int, Prof varchar(12), YearOffered int)";
         stmt.executeUpdate(s);
         System.out.println("Table SECTION created.");

         s = "insert into SECTION(SectId, CourseId, Prof, YearOffered) values ";
         String[] sectvals = {
                 "(13, 12, 'turing', 2018)",
                 "(23, 12, 'turing', 2019)",
                 "(33, 32, 'newton', 2019)",
                 "(43, 32, 'einstein', 2020)",
                 "(53, 62, 'brando', 2018)",
                 "(63, 62, 'turing', 2020)",
                 "(73, 82, 'yoga', 2019)",
                 "(83, 82, 'newton', 2019)",
                 "(93, 102, 'einstein', 2017)",
                 "(103, 112, 'brando', 2018)",
                 "(113, 122, 'turing', 2021)",
                 "(123, 122, 'turing', 2020)",
                 "(133, 132, 'newton', 2019)",
                 "(143, 132, 'einstein', 2020)",
                 "(153, 162, 'brando', 2018)",
                 "(163, 162, 'turing', 2020)",
                 "(173, 182, 'yoga', 2019)",
                 "(183, 182, 'newton', 2020)",
                 "(193, 202, 'einstein', 2017)",
                 "(203, 212, 'brando', 2018)",
                 "(213, 222, 'hartin menz', 2021)",
                 "(223, 222, 'nani', 2020)",
                 "(233, 232, 'newton', 2019)",
                 "(243, 232, 'einstein', 2020)",
                 "(253, 262, 'brando', 2018)",
                 "(263, 262, 'turing', 2020)",
                 "(273, 282, 'yoga', 2019)",
                 "(283, 282, 'newton', 2020)",
                 "(293, 302, 'einstein', 2017)",
                 "(303, 312, 'brando', 2018)",
                 "(313, 322, 'hartin menz', 2021)",
                 "(323, 322, 'nani', 2020)",
                 "(333, 332, 'newton', 2019)",
                 "(343, 332, 'einstein', 2020)",
                 "(353, 362, 'brando', 2019)",
                 "(363, 362, 'hartin menz', 2020)",
                 "(373, 382, 'hartin menz', 2019)",
                 "(383, 382, 'newton', 2020)",
                 "(393, 302, 'einstein', 2022)",
                 "(403, 412, 'brando', 2018)",
                 "(413, 422, 'hartin menz', 2021)",
                 "(423, 422, 'nani', 2020)",
                 "(433, 432, 'newton', 2019)",
                 "(443, 432, 'einstein', 2020)",
                 "(453, 462, 'brando', 2019)",
                 "(463, 462, 'hartin menz', 2020)",
                 "(473, 482, 'hartin menz', 2019)",
                 "(483, 482, 'newton', 2020)",
                 "(493, 402, 'einstein', 2022)",
                 "(503, 512, 'brando', 2018)"
         };
         for (int i=0; i<sectvals.length; i++)
            stmt.executeUpdate(s + sectvals[i]);
         System.out.println("SECTION records inserted.");

         s = "create table ENROLL(EId int, StudentId int, SectionId int, Grade varchar(2))";
         stmt.executeUpdate(s);
         System.out.println("Table ENROLL created.");

         s = "create index idxStudentId on enroll(studentid) using btree";
         stmt.executeUpdate(s);
         System.out.println("index idxStudentId on enroll(studentid) using btree");

         s = "insert into ENROLL(EId, StudentId, SectionId, Grade) values ";
         String[] enrollvals = {
                 "(14, 1, 13, 'A')",
                 "(24, 1, 43, 'C' )",
                 "(34, 2, 43, 'B+')",
                 "(44, 2, 33, 'B' )",
                 "(54, 3, 53, 'A' )",
                 "(64, 3, 53, 'A' )",
                 "(74, 4, 13, 'B')",
                 "(84, 4, 43, 'F' )",
                 "(94, 5, 43, 'B+')",
                 "(104, 5, 33, 'A-' )",
                 "(114, 6, 53, 'A' )",
                 "(124, 6, 63, 'B-' )",
                 "(134, 7, 73, 'B+')",
                 "(144, 7, 83, 'B' )",
                 "(154, 8, 83, 'A' )",
                 "(164, 8, 93, 'A' )",
                 "(174, 9, 73, 'B')",
                 "(184, 9, 93, 'F' )",
                 "(194, 10, 103, 'B+')",
                 "(204, 10, 73, 'A-' )",
                 "(214, 11, 83, 'A' )",
                 "(224, 11, 93, 'B-' )",
                 "(234, 12, 103, 'B+')",
                 "(244, 12, 113, 'B' )",
                 "(254, 13, 103, 'A' )",
                 "(264, 13, 113, 'A+' )",
                 "(274, 14, 113, 'B+')",
                 "(284, 14, 103, 'D' )",
                 "(294, 15, 133, 'B+')",
                 "(304, 15, 123, 'A-' )",
                 "(314, 16, 123, 'A' )",
                 "(324, 16, 133, 'B-' )",
                 "(334, 17, 133, 'B+')",
                 "(344, 17, 123, 'B' )",
                 "(354, 18, 143, 'A' )",
                 "(364, 18, 153, 'A+' )",
                 "(374, 19, 143, 'B+')",
                 "(384, 19, 153, 'A' )",
                 "(394, 20, 143, 'B+')",
                 "(404, 20, 153, 'A-' )",
                 "(414, 21, 163, 'A' )",
                 "(424, 21, 173, 'B-' )",
                 "(434, 22, 183, 'B+')",
                 "(444, 22, 163, 'B' )",
                 "(454, 23, 173, 'A' )",
                 "(464, 23, 183, 'A+' )",
                 "(474, 24, 193, 'B+')",
                 "(484, 24, 203, 'A' )",
                 "(494, 25, 193, 'B+')",
                 "(504, 25, 203, 'A-' )"
         };
         
         for (int i=0; i<enrollvals.length; i++)
            stmt.executeUpdate(s + enrollvals[i]);
         System.out.println("ENROLL records inserted.");
      }
      catch(SQLException e) {
         e.printStackTrace();
      }
   }
}
