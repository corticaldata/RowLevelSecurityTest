package test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Date;
import java.util.Random;

public class Test2 {
	public static void main(String[] args) throws Exception {
		Connection connection = DriverManager.getConnection("jdbc:postgresql://localhost:5432/backoffice", "postgres", "");
		
		StringBuffer sql = new StringBuffer();
		sql.append("SELECT array_to_json(array_agg(row_to_json(q1))) AS json ")
			.append("FROM ( ")
			.append("SELECT ")
			.append("	dg.name_ AS name, ")
			.append("					CASE WHEN count(q2) > 0 THEN array_to_json(array_agg(row_to_json(q2))) ELSE NULL END AS domains ")
			.append("				FROM data_governance_ AS dg ")
			.append("				LEFT JOIN LATERAL ( ")
			.append("					SELECT ")
			.append("						d.name_ AS name, ")
			.append("						CASE WHEN count(q3) > 0 THEN array_to_json(array_agg(row_to_json(q3))) ELSE NULL END AS entities ")
			.append("					FROM domain_ AS d ")
			.append("					LEFT JOIN LATERAL ( ")
			.append("						SELECT  ")
			.append("							e.name_ AS name, ")
			.append("							CASE WHEN count(q4) > 0 THEN array_to_json(array_agg(row_to_json(q4))) ELSE NULL END AS attributes ")
			.append("						FROM entity_ AS e ")
			.append("						LEFT JOIN LATERAL ( ")
			.append("							SELECT ")
			.append("								a.name_ AS name ")
			.append("							FROM attribute_ AS a ")
			.append("							WHERE a._entity_id_ = e._id_ ")
			.append("						) q4 ON TRUE ")
			.append("						WHERE e._domain_id_ = d._id_ ")
			.append("						GROUP BY e.name_ ")
			.append("					) q3 ON TRUE ")
			.append("					WHERE d._id_ = 1 ")
			.append("					GROUP BY d.name_ ")
			.append("				) q2 ON TRUE ")
			.append("				GROUP BY dg.name_ ")
			.append("			) q1; ");
		
		PreparedStatement ps = connection.prepareStatement(sql.toString());
		
		long time1 = new Date().getTime();
		for (int i = 0; i < 100000; i++) {
			
			if (i % 1000 == 0) {
				System.out.println("Requests: " + i);
			}

			ResultSet rs = ps.executeQuery();
			while (rs.next()) {
				//System.out.println(rs.getString("json"));
			}
			rs.close();
		}
		long time2 = new Date().getTime();
		ps.close();
		connection.close();
		System.out.println(100000.0 / (time2 - time1));
	}
}
