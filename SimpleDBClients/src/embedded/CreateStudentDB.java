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
         String s = "create table STUDENT(SId int, SName varchar(10), MajorId int, GradYear int)";
         stmt.executeUpdate(s);
         System.out.println("Table STUDENT created.");

         s = "create index idxMajor on student(majorid) using hash";
         stmt.executeUpdate(s);
         System.out.println("index idxMajor created on student(majorid) hash");

         s = "insert into STUDENT(SId, SName, MajorId, GradYear) values ";
         String[] studvals = {"(1, 'joe', 10, 2021)",
               "(2, 'amy', 20, 2020)",
               "(3, 'max', 10, 2022)",
               "(4, 'sue', 20, 2022)",
               "(5, 'bob', 30, 2020)",
               "(6, 'kim', 20, 2020)",
               "(7, 'art', 30, 2021)",
               "(8, 'pat', 20, 2019)",
         "(9, 'lee', 10, 2021)"};
         for (int i=0; i<studvals.length; i++)
            stmt.executeUpdate(s + studvals[i]);
         System.out.println("STUDENT records inserted.");

         s = "create table DEPT(DId int, DName varchar(8))";
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
                 "(502, 'msg', 290)",
         };
         for (int i=0; i<coursevals.length; i++)
            stmt.executeUpdate(s + coursevals[i]);
         System.out.println("COURSE records inserted.");

         s = "create table SECTION(SectId int, CourseId int, Prof varchar(8), YearOffered int)";
         stmt.executeUpdate(s);
         System.out.println("Table SECTION created.");

         s = "insert into SECTION(SectId, CourseId, Prof, YearOffered) values ";
         String[] sectvals = {"(13, 12, 'turing', 2018)",
                              "(23, 12, 'turing', 2019)",
                              "(33, 32, 'newton', 2019)",
                              "(43, 32, 'einstein', 2017)",
                              "(53, 62, 'brando', 2018)"};
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
         String[] enrollvals = {"(14, 1, 13, 'A')",
                                "(24, 1, 43, 'C' )",
                                "(34, 2, 43, 'B+')",
                                "(44, 4, 33, 'B' )",
                                "(54, 4, 53, 'A' )",
                                "(64, 6, 53, 'A' )"};
         for (int i=0; i<enrollvals.length; i++)
            stmt.executeUpdate(s + enrollvals[i]);
         System.out.println("ENROLL records inserted.");
      }
      catch(SQLException e) {
         e.printStackTrace();
      }
   }
}
