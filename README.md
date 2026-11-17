# Medicine Tracking System

The JavaScript application provides a local browser-based dashboard for
medicine catalogues, health centres, suppliers, stock, dispensing, deliveries,
transfers, movement history, and inventory reports. Its inventory chart shows
every medicine, including medicines with no recorded stock.

## Run

1. Install Node.js 16.20.1 or later.
2. Start MongoDB.
3. In this folder, run `npm install`.
4. Run `npm start` and open the printed local URL (default:
   `http://127.0.0.1:3000`).

The application reads `med_connect.env` if present. Environment variables take
precedence over that file: `MONGO_URI`, `MONGO_DATABASE`, and `PORT`. The
database defaults to `medicine_tracking_system_db` and the server binds to
localhost only. The included configuration uses `medicine_tracking_system_db`

MongoDB multi-document transactions require a replica set. CRUD and individual
document writes can still be used with a standalone MongoDB server, but
multi-document rollback is unavailable there.

## Check and test

- `npm run check` checks the server JavaScript syntax.
- `npm test` checks the data validation, inventory alerts, sample data, and
  embedded browser application.
