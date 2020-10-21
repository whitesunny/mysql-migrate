package com.zxm.migrate;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;

import java.math.BigDecimal;
import java.sql.*;
import java.text.SimpleDateFormat;

/**
 * @author Jason
 */
public class WriteToMysql {

    public static void main(String[] args) throws SQLException {

        Connection conn = null;
        PreparedStatement stmt = null;

        DruidDataSource ds = new DruidDataSource();
        ds.setDriverClassName("com.mysql.jdbc.Driver");
        ds.setUrl("jdbc:mysql:///mysql_copy?useUnicode=true&characterSetEncoding=utf8");
        ds.setUsername("root");
        ds.setPassword("root");

        try {
            conn = ds.getConnection();
            conn.setAutoCommit(false);
            String sql = "insert into t_test values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
            stmt = conn.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            stmt.setFetchSize(Integer.MIN_VALUE);

            int j = 0;
            for (int i = 0; i < 10006666; i++) {

                j++;
                Integer id_tinyint = i % 127;
                Integer id_smallint = i % 32767;
                Integer id_mediumint = i;
                Integer id_int = i;
                Integer id_bigint = i;
                BigDecimal id_decimal = new BigDecimal("12.67");
                BigDecimal id_numeric = new BigDecimal("19.999");

                Float id_float = 1912f;
                Double id_double = 12.854d;

                Integer b_bit = 199;

                Date d_date = new Date(new java.util.Date().getTime());
                Time t_time = new Time(new java.util.Date().getTime());
                String d_datetime = new Timestamp(new java.util.Date().getTime()).toString();
                String t_timestamp = new Timestamp(new java.util.Date().getTime()).toString();
                String year = new SimpleDateFormat("yyyy").format(new java.util.Date());

                String c_char = "淐";
                String v_varchar = "溚溛溞溟溠溡溣溤溥溦溧溨溩溬溭溯溰溱溲涢溴溵溶溷溸溹";
                String t_tinytext = "澭浍澯澰淀澲澳澴澵澶澷澸澹澺澻澼澽澾澿濂濄";
                String t_text = "熵熶熷熸熹熺熻熼熽炽熿燀烨燂燅燆燇炖燊燋燌燍燎燏";
                String m_mediumtext = "烣烥烩烪烯烰烱烲烳烃烵烶烷烸烹烺烻烼烾烿焀焁焂焃";
                String l_longtext = "爚烂爜爝爞爟爠爡爢爣爤爥爦爧爨爩";
                String b_binary = "焯焱焲焳焴焵焷焸焹焺焻焼焽焾焿煀煁煂煃煄煅";
                String v_varbinary = "焯焱焲焳焴焵焷焸焹焺焻焼焽焾焿煀煁煂煃煄煅";
                String t_tinyblob = "獞獟獠獡獢獣獤獥獦獧獩狯猃獬獭狝獯狞獱獳獴獶獹獽獾獿猡玁";
                String b_blob = "狘狁狃狄狅狆狇狉狊狋狌狍狎狏狑狒狓狔";
                String m_mediumblob = "焘燿爀爁爂爃爄爅爇爈爉爊爋爌烁爎爏爑爒爓爔爕";
                String l_longblob = "犠犡犣犤犥犦牺犨犩犪犫";
                String e_enum = "large";
                String s_set = "1";

                stmt.setObject(1,id_tinyint);
                stmt.setObject(2,id_smallint);
                stmt.setObject(3,id_mediumint);
                stmt.setObject(4,id_int);
                stmt.setObject(5,id_bigint);
                stmt.setObject(6,id_decimal);
                stmt.setObject(7,id_numeric);
                stmt.setObject(8,id_float);
                stmt.setObject(9,id_double);
                stmt.setObject(10,b_bit);
                stmt.setObject(11,d_date);
                stmt.setObject(12,t_time);
                stmt.setObject(13,d_datetime);
                stmt.setObject(14,t_timestamp);
                stmt.setObject(15,year);
                stmt.setObject(16,c_char);
                stmt.setObject(17,v_varchar);
                stmt.setObject(18,t_tinytext);
                stmt.setObject(19,t_text);
                stmt.setObject(20,m_mediumtext);
                stmt.setObject(21,l_longtext);
                stmt.setObject(22,b_binary);
                stmt.setObject(23,v_varbinary);
                stmt.setObject(24,t_tinyblob);
                stmt.setObject(25,b_blob);
                stmt.setObject(26,m_mediumblob);
                stmt.setObject(27,l_longblob);
                stmt.setObject(28,e_enum);
                stmt.setObject(29,s_set);

                stmt.addBatch();
                if(j >= 500){
                    stmt.executeBatch();
                    conn.commit();
                    j = 0;
                }
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

        try {
            stmt.executeBatch();
            conn.commit();
        } catch (SQLException e) {
            e.printStackTrace();
            if (conn != null) {
                conn.rollback();
            }
        }

    }
}
