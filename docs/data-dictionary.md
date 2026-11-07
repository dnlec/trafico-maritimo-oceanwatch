# Automatic Identification System  
## Broadcast Points Data Dictionaries

**Updated:** July 31, 2026

---

### Data Dictionary (2018 to 2024)

This data dictionary pertains to point data from 2018 to 2024.

| # | Name | Description | Example | Units | Resolution | Type | Size |
|---|------|-------------|---------|-------|------------|------|------|
| 1 | MMSI | Maritime Mobile Service Identity value | 477220100 | | | Text | 9 |
| 2 | BaseDateTime | Full UTC date and time | 2017-02-01T20:05:07 | | YYYY-MM-DD:HH-MM-SS | DateTime | |
| 3 | LAT | Latitude | 42.35137 | decimal degrees | XX.XXXXX | Double | 8 |
| 4 | LON | Longitude | -71.04182 | decimal degrees | XXX.XXXXX | Double | 8 |
| 5 | SOG | Speed Over Ground | 5.9 | knots | XXX.X | Float | 4 |
| 6 | COG | Course Over Ground | 47.5 | degrees | XXX.X | Float | 4 |
| 7 | Heading | True Heading | 45.1 | degrees | XXX.X | Float | 4 |
| 8 | VesselName | Name as shown on the station radio license | OOCL Malaysia | | | Text | 32 |
| 9 | IMO | International Maritime Organization Vessel number | IMO9627980 | | | Text | 10 |
| 10 | CallSign | Call sign as assigned by FCC | VRME7 | | | Text | 8 |
| 11 | VesselType | Vessel type as defined in NAIS specifications | 70 | | | Integer | short |
| 12 | Status | Navigation status | 3 | | | Integer | short |
| 13 | Length | Length of vessel (see NAIS specifications) | 71.0 | meters | XXX.X | Float | 4 |
| 14 | Width | Width of vessel (see NAIS specifications) | 12.0 | meters | XXX.X | Float | 4 |
| 15 | Draft | Draft depth of vessel (see NAIS specifications) | 3.5 | meters | XXX.X | Float | 4 |
| 16 | Cargo | Cargo type (see NAIS specification and codes) | 70 | | | Text | 4 |
| 17 | TransceiverClass | Class of AIS transceiver – beginning in 2018 | A | | | Text | 2 |

**Marine Cadastre**

---

### Data Dictionary (2025 to present)

This data dictionary pertains to point data from 2025 to present.

| # | Field Name | Description | Example | Unit | Valid Domain | Null Allowed | Type | Bytes |
|---|------------|-------------|---------|------|--------------|--------------|------|-------|
| 1 | mmsi | Maritime Mobile Service Identity value | 477220100 | integer | 2-7 + MIDx3 + 4 | N | int32 | 4 |
| 2 | base_date_time | Full UTC date and time | 2017-02-01T20:05:07 | - | - | N | datetime64[ns] | 8 |
| 3 | longitude | Longitude | -71.04182 | decimal degree | -179.99999 to 179.99999 | N | double | 8 |
| 4 | latitude | Latitude | 42.35137 | decimal degree | -89.99999 to 89.99999 | N | double | 8 |
| 5 | sog | Speed Over Ground | 5.9 | knot | 0 to 99.9 | Y | float | 4 |
| 6 | cog | Course Over Ground | 47.5 | degree NAz | 0 to 359.9 | Y | float | 4 |
| 7 | heading | True Heading | 45 | degree NAz | 0 to 359 | Y | int32 | 4 |
| 8 | vessel_name | Name as shown on the station radio license | OOCL Malaysia | alphanumeric ASCII characters UTF-8 | | Y | string | 24 |
| 9 | imo | International Maritime Organization Vessel number | IMO9627980 | alphanumeric | alphanumeric | Y | string | 12 |
| 10 | call_sign | Call sign as assigned by FCC | VRME7 | alphanumeric | alphanumeric | Y | string | 8 |
| 11 | vessel_type | Vessel type as defined in NAIS specifications | 70 | scalar | 1 to 1024* | Y | int32 | 4 |
| 12 | status | Navigation status | 3 | scalar | 1 to 14* | Y | int32 | 4 |
| 13 | length | Length of vessel (see NAIS specifications) | 71 | meter | 1 to 509 | Y | int32 | 4 |
| 14 | width | Width of vessel (see NAIS specifications) | 12 | meter | 1 to 61 | Y | int32 | 4 |
| 15 | draft | Draft depth of vessel (see NAIS specifications) | 3.5 | meter | 1 to 24 | Y | float | 4 |
| 16 | cargo | Cargo type (see NAIS specification and codes) | 70 | scalar | 1 to 1024* | Y | int32 | 4 |
| 17 | transceiver | Class of AIS transceiver | A | character | A \| B | Y | string | 2 |

\*Applicable lookup table required

**Marine Cadastre**