const mysql = require('mysql2/promise');
const crypto = require('crypto');

(async () => {
  const conn = await mysql.createConnection({
    host: process.env.MYSQL_HOST || 'mysql',
    user: process.env.MYSQL_USER || 'root',
    password: process.env.MYSQL_PASSWORD || 'example',
    database: process.env.MYSQL_DATABASE || 'medigrid'
  });

  const [initialCount] = await conn.query('SELECT count(*) as count FROM hospitals');
  console.log('Current hospital count in MySQL:', initialCount[0].count);

  const newHospitals = [
    {
      id: crypto.randomUUID(),
      name: 'Sant Joan de Déu Children Emergency Pavilion',
      lat: 42.5080,
      lon: 1.5310,
      phone: '+376 890 100',
      address: 'Avinguda Doctor Mitjavila 12, Andorra la Vella',
      bedCount: 50
    },
    {
      id: crypto.randomUUID(),
      name: 'Soldeu Alpine Trauma & Resuscitation Center',
      lat: 42.5760,
      lon: 1.6670,
      phone: '+376 890 200',
      address: 'Carretera General 2, AD100 Soldeu',
      bedCount: 40
    },
    {
      id: crypto.randomUUID(),
      name: 'Vallnord Rapid Response Acute Hospital',
      lat: 42.5450,
      lon: 1.5180,
      phone: '+376 890 300',
      address: 'Carrer Major 45, La Massana',
      bedCount: 60
    }
  ];

  const bedTypes = ['ICU', 'VENTILATOR', 'GENERAL', 'PEDIATRIC'];

  for (const h of newHospitals) {
    await conn.query(
      'INSERT INTO hospitals (id, name, lat, lon, phone, address) VALUES (?, ?, ?, ?, ?, ?)',
      [h.id, h.name, h.lat, h.lon, h.phone, h.address]
    );

    const bedRows = [];
    for (let b = 1; b <= h.bedCount; b++) {
      const bedId = crypto.randomUUID();
      const bType = bedTypes[b % bedTypes.length];
      const status = 'FREE';
      bedRows.push([bedId, h.id, bType, status, 1]);
    }

    await conn.query(
      'INSERT INTO beds (id, hospital_id, bed_type, status, version) VALUES ?',
      [bedRows]
    );

    console.log(`Inserted: ${h.name} (${h.bedCount} beds)`);
  }

  const [finalHosp] = await conn.query('SELECT count(*) as count FROM hospitals');
  const [finalBeds] = await conn.query('SELECT count(*) as count FROM beds');
  console.log('Updated total hospitals in DB:', finalHosp[0].count);
  console.log('Updated total beds in DB:', finalBeds[0].count);

  await conn.end();
})().catch(console.error);

