# Snowflake Exporter custom integration

![GitHub Release](https://img.shields.io/github/release/nzregs/snowflake-hacs.svg?style=plastic) ![GitHub issues](https://img.shields.io/github/issues/nzregs/snowflake-hacs.svg?style=plastic) ![GitHub Stars](https://img.shields.io/github/stars/nzregs/snowflake-hacs.svg?style=plastic) ![GitHub Last Commit](https://img.shields.io/github/last-commit/nzregs/snowflake-hacs.svg?style=plastic) ![HACS Status](https://img.shields.io/badge/HACS-Custom-orange.svg?style=plastic) ![Home Assistant](https://img.shields.io/badge/Home%20Assistant-%3E%3D%202026.3.0-brightgreen.svg?style=plastic)

<p align="center">
  <img src="custom_components/snowflake_exporter/brand/icon@2x.png" alt="Snowflake Exporter" width="128">
</p>

The Snowflake Exporter custom integration replicates Home Assistant entity states to a Snowflake table on a schedule, using key-pair authentication, the Snowflake SQL API, and Snowpipe Streaming.

It creates these tables in your chosen database and schema:

- **HA_CONFIGURATION** – your Home Assistant instance details (one row per home)
- **HA_ENTITY_STATES** – entity state, attributes, `last_changed` and `last_updated`, appended on every sync
- **HA_ENTITIES** – one row per synced entity: integration, device, area, name, device class, unit, category and labels
- **HA_DEVICES** – one row per device behind those entities (and any parent hubs or bridges): manufacturer, model, serial number, firmware, MAC address, IP address (when the device's configuration URL contains one), parent device and integrations
- **HA_AREAS** – one row per area used by those entities and devices, with its floor

The three registry tables are kept in step with Home Assistant: they are refreshed at startup and about 30 seconds after any entity, device, area or floor change, and rows for removed items are deleted. Join them to `HA_ENTITY_STATES` on `entity_id`, `device_id` and `area_id`. For example:

```sql
SELECT s.entity_id, s.state, s.last_updated,
       e.device_class, e.integration,
       d.manufacturer, d.model, d.mac_address,
       a.name AS area, a.floor_name
FROM HA_ENTITY_STATES s
JOIN HA_ENTITIES e ON e.entity_id = s.entity_id
LEFT JOIN HA_DEVICES d ON d.device_id = e.device_id
LEFT JOIN HA_AREAS a ON a.area_id = e.area_id;
```

It also sets up 8 sensors to monitor replication:

- **Replication Frequency**
- **Replication Mode**
- **Replicated Entities**
- **Next Replication**
- **Previous Replication Started**
- **Previous Replication Finished**
- **Previous Replication Status**
- **Previous Replication Message**

---

## **Installing the Snowflake Exporter Custom Integration in Home Assistant via HACS**

This guide will walk you through the steps to install and set up the Snowflake Exporter custom integration in Home Assistant via the Home Assistant Community Store (HACS).

### **Prerequisites**

- **Home Assistant** 2026.3.0 or later must already be installed and configured on your system.
- **HACS** should be installed. If not, follow the instructions at [HACS Installation](https://www.hacs.xyz/docs/use/download/prerequisites/).
- A **Snowflake account**, plus:
  - A Snowflake **user** for Home Assistant whose default role can use the warehouse, database and schema, and can create and insert into tables in that schema.
  - Your **account identifier** (e.g. `ORGNAME-ACCOUNT`) and **account locator** (found under **Admin > Accounts** in Snowsight).
  - To create all of these in one go, edit the variables at the top of [`sql/setup_snowflake.sql`](sql/setup_snowflake.sql) (database, schema, user, role, network policy and allowed IPs) and run it in Snowsight. It sets up a key-pair-only service user, a role with the grants and future grants the integration needs, and a network policy that limits sign-in to your IP addresses.
- Database, schema and warehouse names must be plain identifiers: start with a letter or underscore, and contain only letters, digits, `_` or `$`.

### **Step-by-Step Guide**

#### **1. Open HACS**

- In Home Assistant, navigate to the side menu and click on **"HACS"**.

#### **2. Add Custom Repository**

- Click on the **three dots menu** at the top right in HACS.
- Select **"Custom Repositories"**.

#### **3. Add Repository**

- In the **"Add Custom Repository URL"** field, paste the following URL:
  ```
  https://github.com/nzregs/snowflake-hacs
  ```
- Under **"Category"**, select **"Integration"**.
- Click on **"Add"**.

#### **4. Install Integration**

- After adding, the integration should be available in HACS.
- Search for **"Snowflake Exporter"** in the HACS integrations list.
- Click on the integration and then click **"Download"**.
- Confirm the installation by clicking **"Download"**.

#### **5. Restart Home Assistant**

- After installation, you need to restart Home Assistant to load the integration.
- Go to **"Settings" > "System" > "Restart"** and click on **"Restart"**.

#### **6. Set Up Integration**

- After the restart, navigate to **"Settings" > "Devices & Services"**.
- Click on **"Add Integration"** (the **"+"** symbol at the bottom right).
- Search for **"Snowflake Exporter"**.
- Select the integration from the list.

#### **7. Enter Connection Details**

- Enter your **Account Identifier**, **Account Locator**, **Username**, **Database**, **Schema** and **Warehouse**.
- Click **"Submit"**.

#### **8. Register the Public Key**

- The integration generates an RSA key pair and shows an `ALTER USER ... SET RSA_PUBLIC_KEY='...'` statement.
- Run that statement in Snowflake (as a role that can alter the user), then click **"Submit"**.
- The integration tests the connection before continuing.

#### **9. Choose Entities**

- Select **"All entities"** or **"Select specific entities"**.
- If selecting specific entities, pick them from the list and click **"Submit"**.

#### **10. Verify Installation**

- The first replication runs immediately, then every 5 minutes by default.
- View the replication sensors under **"Settings" > "Devices & Services" > "Snowflake Exporter"**.
- Query `HA_ENTITY_STATES` in Snowflake to confirm data is arriving.

## **Configuration**

To change the connection settings, sync interval or entity selection:

- Go to **"Settings" > "Devices & Services"** and find **Snowflake Exporter**.
- Click **"Configure"**, update the settings, and click **"Submit"**.

## **Security and Privacy Warnings**

> [!WARNING]
> **Your home's location is sent to Snowflake.** `HA_CONFIGURATION` stores your Home Assistant location name, latitude, longitude, elevation, time zone and country. Anyone with read access to that table can see where your home is.

> [!WARNING]
> **The private key is stored unencrypted.** The generated private key is kept in plain text in Home Assistant's `.storage/core.config_entries` file, and is included in every Home Assistant backup. Anyone with that file or a backup can sign in to Snowflake as this user. Use a **dedicated Snowflake user** with a role limited to the one warehouse and schema it needs, and protect your backups.

> [!WARNING]
> **Connection details and errors are visible in Home Assistant.** The **Replication Mode** sensor shows your account, database, schema and warehouse as attributes, and **Previous Replication Message** can contain raw Snowflake error text. Both are visible to every Home Assistant user and are kept in history.

> [!WARNING]
> **Device identifiers are sent to Snowflake.** `HA_DEVICES` stores device MAC addresses, IP addresses, serial numbers and configuration URLs for the synced entities. Treat the table as sensitive network inventory.

> [!NOTE]
> In **"All entities"** mode, every attribute of every entity is exported apart from `access_token` and `entity_picture`. That can include GPS coordinates from `device_tracker` and `person` entities. Use **"Select specific entities"** if you don't want everything exported.

## **Notes**

If you have any issues with the Snowflake Exporter entities, please try to Reload the integration.
To reload a HACS integration in Home Assistant, follow these steps:

- Open Home Assistant and go to Settings > Devices & Services.
- Find the integration you want to reload.
- Click on the three-dot menu (⋮) next to the integration.
- Select Reload from the menu.
- If the integration doesn't support reloading, you might need to restart Home Assistant.

Report bugs and feature requests at [https://github.com/nzregs/snowflake-hacs/issues](https://github.com/nzregs/snowflake-hacs/issues).
