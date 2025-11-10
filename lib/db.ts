import postgres from 'postgres'

const DATABASE_URL = process.env.DATABASE_URL

if (!DATABASE_URL) {
	throw new Error('Faltan las variables de entorno DATABASE_URL')
}

const sql = postgres(DATABASE_URL, {
  ssl: 'require',
});

export default sql