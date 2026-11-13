import mysql from 'mysql2/promise';
import { randomUUID } from 'crypto';

/**
 * Seed generator creating 50 real-world Hyderabad hospitals and 5,000 beds.
 * Coordinates are mapped onto authentic Hyderabad geographical landmarks and road corridors.
 */
interface HospitalSeed {
  name: string;
  lat: number;
  lon: number;
  phone: string;
  address: string;
}

const hyderabadHospitals: HospitalSeed[] = [
  { name: "Nizam's Institute of Medical Sciences (NIMS)", lat: 17.4222, lon: 78.4526, phone: "+91 40 2348 9000", address: "Punjagutta, Somajiguda, Hyderabad" },
  { name: "Osmania General Hospital", lat: 17.3753, lon: 78.4744, phone: "+91 40 2460 0146", address: "Afzal Gunj, High Court Road, Hyderabad" },
  { name: "Gandhi Hospital", lat: 17.4243, lon: 78.5036, phone: "+91 40 2750 5566", address: "Musheerabad, Secunderabad, Hyderabad" },
  { name: "Apollo Hospitals Jubilee Hills", lat: 17.4162, lon: 78.4120, phone: "+91 40 2360 7777", address: "Road No. 72, Film Nagar, Jubilee Hills, Hyderabad" },
  { name: "Yashoda Hospitals Somajiguda", lat: 17.4262, lon: 78.4578, phone: "+91 40 4567 4567", address: "Raj Bhavan Rd, Somajiguda, Hyderabad" },
  { name: "Yashoda Hospitals Secunderabad", lat: 17.4414, lon: 78.4983, phone: "+91 40 2771 3333", address: "Alexander Rd, Clock Tower, Secunderabad" },
  { name: "Yashoda Hospitals Malakpet", lat: 17.3736, lon: 78.4982, phone: "+91 40 2455 5555", address: "Nalgonda X Roads, Malakpet, Hyderabad" },
  { name: "KIMS Hospitals Secunderabad", lat: 17.4367, lon: 78.4878, phone: "+91 40 4488 5000", address: "1-8-31/1, Minister Rd, Krishna Nagar Colony, Secunderabad" },
  { name: "KIMS Hospitals Kondapur", lat: 17.4649, lon: 78.3615, phone: "+91 40 6750 5000", address: "Gachibowli-Miyapur Road, Kondapur, Hyderabad" },
  { name: "CARE Hospitals Banjara Hills", lat: 17.4124, lon: 78.4482, phone: "+91 40 6165 6565", address: "Road No. 1, Banjara Hills, Hyderabad" },
  { name: "CARE Hospitals HITEC City", lat: 17.4401, lon: 78.3683, phone: "+91 40 6165 7575", address: "Jayabheri Pine Valley, Old Mumbai Hwy, Gachibowli" },
  { name: "CARE Hospitals Nampally", lat: 17.3912, lon: 78.4716, phone: "+91 40 3041 8000", address: "Exhibition Grounds Rd, Mukarramjahi Rd, Nampally" },
  { name: "Continental Hospitals", lat: 17.4166, lon: 78.3444, phone: "+91 40 6700 0000", address: "Plot No. 3, Financial District, Nanakramguda, Gachibowli" },
  { name: "AIG Hospitals Gachibowli", lat: 17.4435, lon: 78.3659, phone: "+91 40 4244 4222", address: "1-66/A, Mindspace Rd, Gachibowli, Hyderabad" },
  { name: "Basavatarakam Indo-American Cancer Hospital", lat: 17.4215, lon: 78.4312, phone: "+91 40 2355 1235", address: "Road No. 10, Banjara Hills, Hyderabad" },
  { name: "Medicover Hospitals HITEC City", lat: 17.4485, lon: 78.3789, phone: "+91 40 6833 4455", address: "Behind Cyber Towers, Madhapur, Hyderabad" },
  { name: "Medicover Hospitals Secretariat", lat: 17.4061, lon: 78.4682, phone: "+91 40 4940 4940", address: "5-9-22, Sarovar Complex, Secretariat Rd, Saifabad" },
  { name: "Sunshine Hospitals Secunderabad", lat: 17.4447, lon: 78.4947, phone: "+91 40 4455 0000", address: "PG Road, Paradise Circle, Secunderabad" },
  { name: "Sunshine Hospitals Gachibowli", lat: 17.4398, lon: 78.3582, phone: "+91 40 4455 5555", address: "P Janardhan Reddy Nagar, Gachibowli, Hyderabad" },
  { name: "Aster Prime Hospital", lat: 17.4358, lon: 78.4441, phone: "+91 40 4959 4959", address: "Plot No 4, Mythrivanam, Ameerpet, Hyderabad" },
  { name: "Rainbow Children's Hospital Banjara Hills", lat: 17.4187, lon: 78.4346, phone: "+91 40 4241 2345", address: "Road No. 2, Banjara Hills, Hyderabad" },
  { name: "Rainbow Children's Hospital Kondapur", lat: 17.4589, lon: 78.3644, phone: "+91 40 4241 3456", address: "Botanical Garden Rd, Kondapur, Hyderabad" },
  { name: "Rainbow Children's Hospital Secunderabad", lat: 17.4578, lon: 78.5032, phone: "+91 40 4241 4567", address: "Vikrampuri Colony, Karkhana, Secunderabad" },
  { name: "PACE Hospitals Hitec City", lat: 17.4419, lon: 78.3842, phone: "+91 40 4848 6868", address: "Metro Pillar A1060, Hitec City Rd, Madhapur" },
  { name: "Citizens Specialty Hospital", lat: 17.4721, lon: 78.3128, phone: "+91 40 6719 1919", address: "Nallagandla, Serilingampally, Hyderabad" },
  { name: "Aware Gleneagles Global Hospitals", lat: 17.3512, lon: 78.5521, phone: "+91 40 2411 1111", address: "Sagar Road, Bairamalguda, LB Nagar, Hyderabad" },
  { name: "Gleneagles Global Hospital Lakdikapul", lat: 17.4045, lon: 78.4621, phone: "+91 40 2324 4444", address: "6-1-1070/1 to 4, Lakdi-ka-pul, Hyderabad" },
  { name: "OMNI Hospitals Kothapet", lat: 17.3689, lon: 78.5367, phone: "+91 40 3999 4999", address: "Opp. P&T Colony, Dilsukhnagar, Kothapet, Hyderabad" },
  { name: "OMNI Hospitals Nampally", lat: 17.3941, lon: 78.4735, phone: "+91 40 4466 5566", address: "Station Road, Nampally, Hyderabad" },
  { name: "TX Hospitals Uppal", lat: 17.3995, lon: 78.5612, phone: "+91 40 4899 9999", address: "Survey No 77, Beside Metro Pillar 952, Uppal, Hyderabad" },
  { name: "TX Hospitals Banjara Hills", lat: 17.4154, lon: 78.4418, phone: "+91 40 4999 9999", address: "Road No. 12, Banjara Hills, Hyderabad" },
  { name: "Prathima Hospitals Kukatpally", lat: 17.4932, lon: 78.3976, phone: "+91 40 4345 4345", address: "Phase 6, KPHB Colony, Kukatpally, Hyderabad" },
  { name: "Prathima Hospitals Kachiguda", lat: 17.3908, lon: 78.5019, phone: "+91 40 6815 6815", address: "Barkatpura, Kachiguda Station Rd, Hyderabad" },
  { name: "SLG Hospitals Bachupally", lat: 17.5312, lon: 78.3681, phone: "+91 40 2727 2727", address: "Nizampet Rd, Bachupally, Hyderabad" },
  { name: "Mallareddy Narayana Multispeciality Hospital", lat: 17.5389, lon: 78.4412, phone: "+91 40 2378 3000", address: "Suraram X Road, Jeedimetla, Hyderabad" },
  { name: "Virinchi Hospitals Banjara Hills", lat: 17.4098, lon: 78.4497, phone: "+91 40 4699 9999", address: "Virinchi Circle, Road No. 1, Banjara Hills, Hyderabad" },
  { name: "Century Hospital Banjara Hills", lat: 17.4138, lon: 78.4354, phone: "+91 40 2337 7777", address: "Road No. 12, Bhola Nagar, Banjara Hills, Hyderabad" },
  { name: "Premier Hospital Toli Chowki", lat: 17.4012, lon: 78.4115, phone: "+91 40 2356 2233", address: "Humayun Nagar, Mehdipatnam - Toli Chowki Rd, Hyderabad" },
  { name: "Olive Hospital Mehdipatnam", lat: 17.3934, lon: 78.4371, phone: "+91 40 2351 1800", address: "12-2-718/3, Pillar No. 43, Mehdipatnam, Hyderabad" },
  { name: "Image Hospitals Ameerpet", lat: 17.4369, lon: 78.4489, phone: "+91 40 2374 8888", address: "Ameerpet 'X' Road, Ameerpet, Hyderabad" },
  { name: "Ankura Hospital for Women & Children Kukatpally", lat: 17.4891, lon: 78.4012, phone: "+91 40 4910 8108", address: "Near JNTU Metro Station, KPHB, Kukatpally, Hyderabad" },
  { name: "Ankura Hospital for Women & Children Mehdipatnam", lat: 17.3921, lon: 78.4398, phone: "+91 40 4910 8208", address: "Rethibowli Ring Road, Mehdipatnam, Hyderabad" },
  { name: "Government General & Chest Hospital Erragadda", lat: 17.4512, lon: 78.4325, phone: "+91 40 2381 4422", address: "Erragadda, Sanath Nagar, Hyderabad" },
  { name: "Sir Ronald Ross Fever Hospital Nallakunta", lat: 17.3982, lon: 78.5089, phone: "+91 40 2756 5074", address: "Barkatpura, Nallakunta, Hyderabad" },
  { name: "Government Maternity Hospital Petlaburj", lat: 17.3695, lon: 78.4721, phone: "+91 40 2452 4471", address: "Petlaburj, Nayapul, Charminar, Hyderabad" },
  { name: "ESI Hospital Sanath Nagar", lat: 17.4567, lon: 78.4415, phone: "+91 40 2370 1099", address: "Sanath Nagar, Hyderabad" },
  { name: "Area Hospital Golconda", lat: 17.3821, lon: 78.4056, phone: "+91 40 2356 3311", address: "Golconda Fort Road, Hyderabad" },
  { name: "District Hospital King Koti", lat: 17.3925, lon: 78.4832, phone: "+91 40 2475 2233", address: "King Koti Road, Abids, Hyderabad" },
  { name: "Maxivision Super Specialty Eye Hospital", lat: 17.4285, lon: 78.4556, phone: "+91 40 4456 7890", address: "Raj Bhavan Rd, Somajiguda, Hyderabad" },
  { name: "Fernandez Hospital Hyderguda", lat: 17.3963, lon: 78.4820, phone: "+91 40 4022 2300", address: "3-6-282, Opp. Old MLA Quarters, Hyderguda, Hyderabad" }
];

async function seed() {
  const pool = await mysql.createPool({
    host: process.env.MYSQL_HOST || 'localhost',
    user: process.env.MYSQL_USER || 'root',
    password: process.env.MYSQL_PASSWORD || 'example',
    database: process.env.MYSQL_DATABASE || 'medigrid',
  });

  console.log('Seeding full PoC Scale dataset: 50 Real Hyderabad Hospitals & 5,000 Beds in MySQL...');

  await pool.query('DELETE FROM dispatch_assignments');
  await pool.query('DELETE FROM patient_transfers');
  await pool.query('DELETE FROM ambulance_logs');
  await pool.query('DELETE FROM ambulances');
  await pool.query('DELETE FROM beds');
  await pool.query('DELETE FROM hospitals');

  const bedTypes: Array<'ICU' | 'VENTILATOR' | 'GENERAL' | 'PEDIATRIC'> = [
    'ICU', 'VENTILATOR', 'GENERAL', 'GENERAL', 'GENERAL', 'PEDIATRIC'
  ];

  const totalHospitals = hyderabadHospitals.length;
  const bedsPerHospital = 100; // 50 * 100 = 5,000 beds

  for (let i = 0; i < totalHospitals; i++) {
    const hosp = hyderabadHospitals[i];
    const hospId = randomUUID();

    await pool.query(
      'INSERT INTO hospitals (id, name, lat, lon, phone, address) VALUES (?, ?, ?, ?, ?, ?)',
      [hospId, hosp.name, hosp.lat, hosp.lon, hosp.phone, hosp.address]
    );

    // Batch insert 100 beds per hospital
    const bedValues: any[] = [];
    for (let b = 1; b <= bedsPerHospital; b++) {
      const bedId = randomUUID();
      const bType = bedTypes[b % bedTypes.length];
      // 70% free, 30% initially occupied
      const status = Math.random() < 0.3 ? 'OCCUPIED' : 'FREE';
      bedValues.push([bedId, hospId, bType, status, 1]);
    }

    await pool.query(
      'INSERT INTO beds (id, hospital_id, bed_type, status, version) VALUES ?',
      [bedValues]
    );

    if ((i + 1) % 10 === 0 || (i + 1) === totalHospitals) {
      console.log(`Generated ${i + 1}/${totalHospitals} hospitals...`);
    }
  }

  // Seed 30 active ambulances spread realistically across Hyderabad key junctions
  const ambulanceLocations = [
    { area: 'Cyber Towers Madhapur', lat: 17.4504, lon: 78.3808 },
    { area: 'Secunderabad Station', lat: 17.4344, lon: 78.5013 },
    { area: 'Charminar Old City', lat: 17.3616, lon: 78.4747 },
    { area: 'Panjagutta Circle', lat: 17.4258, lon: 78.4513 },
    { area: 'Gachibowli ORR', lat: 17.4400, lon: 78.3489 },
    { area: 'Jubilee Hills Checkpost', lat: 17.4283, lon: 78.4111 },
    { area: 'Mehdipatnam X Roads', lat: 17.3916, lon: 78.4410 },
    { area: 'Dilsukhnagar Metro', lat: 17.3688, lon: 78.5247 },
    { area: 'LB Nagar Junction', lat: 17.3457, lon: 78.5522 },
    { area: 'Kukatpally Y Junction', lat: 17.4842, lon: 78.4138 },
    { area: 'Ameerpet Metro', lat: 17.4375, lon: 78.4482 },
    { area: 'Begumpet Airport Rd', lat: 17.4441, lon: 78.4682 },
    { area: 'Uppal Ring Road', lat: 17.4018, lon: 78.5602 },
    { area: 'Miyapur Metro Terminal', lat: 17.4968, lon: 78.3614 },
    { area: 'Lakdikapul Station', lat: 17.4050, lon: 78.4632 },
    { area: 'Nampally Station', lat: 17.3920, lon: 78.4697 },
    { area: 'Koti Women College', lat: 17.3842, lon: 78.4878 },
    { area: 'Tarnaka X Roads', lat: 17.4281, lon: 78.5312 },
    { area: 'Financial District', lat: 17.4150, lon: 78.3420 },
    { area: 'Kondapur Botanical Garden', lat: 17.4610, lon: 78.3620 },
    { area: 'Erragadda Metro', lat: 17.4490, lon: 78.4350 },
    { area: 'Paradise Secunderabad', lat: 17.4430, lon: 78.4930 },
    { area: 'Golconda Fort Entrance', lat: 17.3833, lon: 78.4011 },
    { area: 'Bachupally X Roads', lat: 17.5320, lon: 78.3700 },
    { area: 'Nallakunta Fever Clinic', lat: 17.3990, lon: 78.5090 },
    { area: 'Tolichowki Flyover', lat: 17.4030, lon: 78.4120 },
    { area: 'Bowenpally Checkpost', lat: 17.4720, lon: 78.4870 },
    { area: 'Jeedimetla Industrial Zone', lat: 17.5180, lon: 78.4530 },
    { area: 'Shamshabad ORR Exit', lat: 17.2403, lon: 78.4294 },
    { area: 'Hussain Sagar Tank Bund', lat: 17.4239, lon: 78.4738 }
  ];

  for (let a = 1; a <= 30; a++) {
    const actorId = `ambulance-TS-09-EM-${(1000 + a).toString()}`;
    const plate = `TS-09-EM-${1000 + a}`;
    const loc = ambulanceLocations[a - 1];

    await pool.query(
      'INSERT INTO ambulances (actor_id, plate_number, status, last_lat, last_lon) VALUES (?, ?, ?, ?, ?)',
      [actorId, plate, 'AVAILABLE', loc.lat, loc.lon]
    );
  }

  const [hospCount]: any = await pool.query('SELECT count(*) as count FROM hospitals');
  const [bedCount]: any = await pool.query('SELECT count(*) as count FROM beds');
  const [ambCount]: any = await pool.query('SELECT count(*) as count FROM ambulances');

  console.log(`\n================ SEED SUMMARY ================`);
  console.log(`Hospitals Inserted : ${hospCount[0].count} (Real Hyderabad Coordinates)`);
  console.log(`Beds Inserted      : ${bedCount[0].count}`);
  console.log(`Ambulances Inserted: ${ambCount[0].count}`);
  console.log(`==============================================\n`);

  await pool.end();
}

seed().catch((err) => {
  console.error('Seed Error:', err);
  process.exit(1);
});
