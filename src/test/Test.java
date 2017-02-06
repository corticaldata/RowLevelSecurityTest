package test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Date;
import java.util.Random;

/**
 * Después de ver las capacidades de PostgreSQL desde la versión 9.5 relativas a seguridad a nivel de fila (RLS - Row Level Security),
 * y viendo que se cubre prácticamente lo mismo que en el meta-modelo de datacentric que había definido, incluyendo el filtrado de datos,
 * había que hacer una 
 * 
 * Prueba de concepto para ver si sería viable tener un ROLE de la base de datos por cada usuario de la web
 * en un entorno con millones de usuarios, cientos de millones de pedidos, ...
 * 
 * La prueba consiste en cargar millones de usuarios, con cientos de millones de pedidos y múltiples líneas de pedido por cada pedido para
 * después hacer consultas de los pedidos de cada usuario protegidas por una política de PostgreSQL.
 * 
 * Cada usuario sólo puede ver sus pedidos. También se prueba la herencia de ROLEs, creando un grupo "customer" en el que
 * se crean las políticas de filtrado.
 * 
 * De esta manera, se simplifica la solución de seguridad, trasladándola al motor de base de datos. Según
 * la filosofía data-centric, la seguridad debería estar en la misma capa que los datos.
 * 
 * Concretamente:
 * 
 * 4. Access to and security of the data is a responsibility of the data layer, and not managed by applications.
 * 
 * El resultado de la prueba es totalmente satisfactorio.
 * 
 * Conclusión: Mover las reglas de seguridad a la base de datos va a permitir simplificar el resto de capas de la
 * arquitectura, ya que no se van a tener que hacer los filtros adicionales de seguridad en los servicios.
 * 
 * SET ROLE user234234;
 * SELECT * FROM order_;
 * 
 * Y se devolverán sólo aquellos pedidos para los que el usuario tenga permisos.
 * 
 * @author paco
 */
public class Test {
	public static void main(String[] args) throws Exception {
		final int NUMBER_OF_USERS = 10000; //10000000;
		final int NUMBER_OF_ORDERS = 100000; //100000000;
		final int NUMBER_OF_ORDER_LINES = 200000; //200000000;
		
		final int NUMBER_OF_REQUESTS = 10000;
		
		Connection connection = DriverManager.getConnection("jdbc:postgresql://localhost:5432/test_rls");
		
		connection.setAutoCommit(false);
		
		Statement statement = connection.createStatement();
		
		Random random = new Random();
		
		statement.execute("DROP TABLE IF EXISTS order_line_");
		statement.execute("DROP TABLE IF EXISTS order_");
		connection.commit();
		
		statement.execute("CREATE ROLE customer");
		connection.commit();
		
		for (int i = 0; i < NUMBER_OF_USERS; i++) {
			statement.execute("CREATE ROLE user" + i);
			statement.execute("GRANT customer TO user" + i);
			
			if (i % 1000 == 0) {
				System.out.println("Users: " + i);
				connection.commit();
			}
		}
		System.out.println("Users: " + NUMBER_OF_USERS);
		connection.commit();
		
		statement.execute("CREATE TABLE order_ (_id_ SERIAL NOT NULL PRIMARY KEY, _user_ VARCHAR, total_ DOUBLE PRECISION)");
		statement.execute("CREATE TABLE order_line_ (_id_ SERIAL NOT NULL PRIMARY KEY, _order_id_ INTEGER REFERENCES order_(_id_), description_ VARCHAR)");
		
		statement.execute("CREATE INDEX i1 ON order_(_user_)");
		statement.execute("CREATE INDEX i2 ON order_line_(_order_id_)");
		
		statement.execute("GRANT ALL ON ALL TABLES IN SCHEMA public TO customer");
		statement.execute("GRANT ALL ON ALL SEQUENCES IN SCHEMA public TO customer");
		
		statement.execute("ALTER TABLE order_ ENABLE ROW LEVEL SECURITY");
		statement.execute("ALTER TABLE order_line_ ENABLE ROW LEVEL SECURITY");
		
		// Ojo, el cast a text es necesario en este caso. Si no se hace, entonces no se utilizan los índices y se hace un full scan, cayendo el rendimiento radicalmente.
		statement.execute("CREATE POLICY p1 ON order_ FOR ALL TO customer USING (_user_ = current_user::text)");
		statement.execute("CREATE POLICY p1 ON order_line_ FOR ALL TO customer USING (_order_id_ IN (SELECT _id_ FROM order_ WHERE _user_ = current_user::text))");

		connection.commit();
		
		PreparedStatement ps = connection.prepareStatement("INSERT INTO order_ (_user_, total_) VALUES (?, ?)");
		for (int i = 0; i < NUMBER_OF_ORDERS; i++) {
			
			int userNumber = Math.abs(random.nextInt()) % NUMBER_OF_USERS;
			double total = Math.abs(random.nextDouble() * 1000);
			
			ps.setString(1, "user" + userNumber);
			ps.setDouble(2, total);
			ps.execute();
			
			if (i % 1000 == 0) {
				connection.commit();
				System.out.println("Orders: " + i);
			}
		}
		System.out.println("Orders: " + NUMBER_OF_ORDERS);
		ps.close();
		connection.commit();
		
		ps = connection.prepareStatement("INSERT INTO order_line_ (_order_id_, description_) VALUES (?, ?)");
		for (int i = 0; i < NUMBER_OF_ORDER_LINES; i++) {
			
			int orderId = (Math.abs(random.nextInt()) % NUMBER_OF_ORDERS) + 1;
			
			ps.setInt(1, orderId);
			ps.setString(2, "description" + i);
			ps.execute();
			
			if (i % 1000 == 0) {
				connection.commit();
				System.out.println("Lines: " + i);
			}
		}
		System.out.println("Lines: " + NUMBER_OF_ORDER_LINES);
		ps.close();
		connection.commit();
		
		// Pruebas de rendimiento, consultas variadas con usuarios distintos
		
		connection.setAutoCommit(true);
		long time1 = new Date().getTime();
		
		ps = connection.prepareStatement("SELECT count(DISTINCT o._id_) AS orders, count(DISTINCT ol._id_) AS lines FROM order_ o LEFT JOIN order_line_ ol ON o._id_ = ol._order_id_");
		//ps = connection.prepareStatement("SELECT count(DISTINCT o._id_) AS orders, 0 AS lines FROM order_ o");
		//ps = connection.prepareStatement("SELECT _id_ AS orders, 0 AS lines FROM order_ o");
		
		for (int i = 0; i < NUMBER_OF_REQUESTS; i++) {
			
			if (i % 1000 == 0) {
				System.out.println("Requests: " + i);
			}
			
			int userNumber = Math.abs(random.nextInt()) % NUMBER_OF_USERS;
			statement.execute("SET ROLE user" + userNumber);
			
			ResultSet rs = ps.executeQuery();
			if (rs.next()) {
				int orders = rs.getInt("orders");
				int lines = rs.getInt("lines");
				//System.out.println(orders + ", " + lines);
			}
			rs.close();
		}
		long time2 = new Date().getTime();
		ps.close();
		System.out.println("Requests: " + NUMBER_OF_REQUESTS);
		
		System.out.println("Requests/second: " + (NUMBER_OF_REQUESTS * 1000 / (time2 - time1)));
		statement.close();
		connection.close();
	}
}
