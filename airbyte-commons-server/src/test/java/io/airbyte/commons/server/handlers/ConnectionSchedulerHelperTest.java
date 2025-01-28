/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.airbyte.api.model.generated.ConnectionScheduleData;
import io.airbyte.api.model.generated.ConnectionScheduleDataBasicSchedule;
import io.airbyte.api.model.generated.ConnectionScheduleDataBasicSchedule.TimeUnitEnum;
import io.airbyte.api.model.generated.ConnectionScheduleDataCron;
import io.airbyte.api.model.generated.ConnectionScheduleType;
import io.airbyte.api.problems.throwable.generated.CronValidationInvalidExpressionProblem;
import io.airbyte.api.problems.throwable.generated.CronValidationInvalidTimezoneProblem;
import io.airbyte.api.problems.throwable.generated.CronValidationMissingComponentProblem;
import io.airbyte.api.problems.throwable.generated.CronValidationMissingCronProblem;
import io.airbyte.commons.server.converters.ApiPojoConverters;
import io.airbyte.commons.server.handlers.helpers.CatalogConverter;
import io.airbyte.commons.server.handlers.helpers.ConnectionScheduleHelper;
import io.airbyte.commons.server.helpers.CronExpressionHelper;
import io.airbyte.config.BasicSchedule.TimeUnit;
import io.airbyte.config.Schedule;
import io.airbyte.config.StandardSync;
import io.airbyte.config.StandardSync.ScheduleType;
import io.airbyte.config.helpers.FieldGenerator;
import io.airbyte.data.exceptions.ConfigNotFoundException;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.TestClient;
import io.airbyte.persistence.job.WorkspaceHelper;
import io.airbyte.validation.json.JsonValidationException;
import java.util.Collections;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ConnectionSchedulerHelperTest {

  private ConnectionScheduleHelper connectionScheduleHelper;
  private final ApiPojoConverters apiPojoConverters = new ApiPojoConverters(new CatalogConverter(new FieldGenerator(), Collections.emptyList()));
  private final CronExpressionHelper cronExpressionHelper = new CronExpressionHelper();

  private static final String EXPECTED_CRON_TIMEZONE = "UTC";
  private static final String EXPECTED_CRON_EXPRESSION = "0 0 12 * * ?";
  private static final UUID WORKSPACE_ID = UUID.randomUUID();
  private static final UUID ORGANIZATION_ID = UUID.randomUUID();

  @BeforeEach
  void setup() throws JsonValidationException, ConfigNotFoundException {
    final WorkspaceHelper workspaceHelper = mock(WorkspaceHelper.class);
    when(workspaceHelper.getWorkspaceForSourceId(any())).thenReturn(WORKSPACE_ID);
    when(workspaceHelper.getOrganizationForWorkspace(WORKSPACE_ID)).thenReturn(ORGANIZATION_ID);
    final FeatureFlagClient featureFlagClient = mock(TestClient.class);
    connectionScheduleHelper =
        new ConnectionScheduleHelper(apiPojoConverters, cronExpressionHelper, featureFlagClient, workspaceHelper);
  }

  @Test
  void testPopulateSyncScheduleFromManualType() throws JsonValidationException, ConfigNotFoundException {
    final StandardSync actual = new StandardSync();
    connectionScheduleHelper.populateSyncFromScheduleTypeAndData(actual,
        ConnectionScheduleType.MANUAL, null);
    assertTrue(actual.getManual());
    assertEquals(ScheduleType.MANUAL, actual.getScheduleType());
    assertNull(actual.getSchedule());
    assertNull(actual.getScheduleData());
  }

  @Test
  void testPopulateSyncScheduleFromBasicType() throws JsonValidationException, ConfigNotFoundException {
    final StandardSync actual = new StandardSync();
    connectionScheduleHelper.populateSyncFromScheduleTypeAndData(actual,
        ConnectionScheduleType.BASIC, new ConnectionScheduleData()
            .basicSchedule(new ConnectionScheduleDataBasicSchedule()
                .timeUnit(TimeUnitEnum.HOURS)
                .units(1L)));
    assertFalse(actual.getManual());
    assertEquals(ScheduleType.BASIC_SCHEDULE, actual.getScheduleType());
    assertEquals(TimeUnit.HOURS, actual.getScheduleData().getBasicSchedule().getTimeUnit());
    assertEquals(1L, actual.getScheduleData().getBasicSchedule().getUnits());
    // We expect the old format to be dual-written.
    assertEquals(Schedule.TimeUnit.HOURS, actual.getSchedule().getTimeUnit());
    assertEquals(1L, actual.getSchedule().getUnits());
  }

  @Test
  void testPopulateSyncScheduleFromCron() throws JsonValidationException, ConfigNotFoundException {
    final StandardSync actual = new StandardSync();
    connectionScheduleHelper.populateSyncFromScheduleTypeAndData(actual,
        ConnectionScheduleType.CRON, new ConnectionScheduleData()
            .cron(new ConnectionScheduleDataCron()
                .cronTimeZone(EXPECTED_CRON_TIMEZONE)
                .cronExpression(EXPECTED_CRON_EXPRESSION)));
    assertEquals(ScheduleType.CRON, actual.getScheduleType());
    assertEquals(EXPECTED_CRON_TIMEZONE, actual.getScheduleData().getCron().getCronTimeZone());
    assertEquals(EXPECTED_CRON_EXPRESSION, actual.getScheduleData().getCron().getCronExpression());
    assertNull(actual.getSchedule());
  }

  @Test
  void testScheduleValidation() {
    final StandardSync actual = new StandardSync();
    assertThrows(JsonValidationException.class, () -> connectionScheduleHelper.populateSyncFromScheduleTypeAndData(actual,
        ConnectionScheduleType.CRON, null));
    assertThrows(JsonValidationException.class,
        () -> connectionScheduleHelper.populateSyncFromScheduleTypeAndData(actual, ConnectionScheduleType.BASIC, new ConnectionScheduleData()));
    assertThrows(CronValidationMissingCronProblem.class,
        () -> connectionScheduleHelper.populateSyncFromScheduleTypeAndData(actual, ConnectionScheduleType.CRON, new ConnectionScheduleData()));
    assertThrows(CronValidationMissingComponentProblem.class,
        () -> connectionScheduleHelper.populateSyncFromScheduleTypeAndData(actual, ConnectionScheduleType.CRON, new ConnectionScheduleData()
            .cron(new ConnectionScheduleDataCron())));
    assertThrows(CronValidationInvalidTimezoneProblem.class,
        () -> connectionScheduleHelper.populateSyncFromScheduleTypeAndData(actual, ConnectionScheduleType.CRON, new ConnectionScheduleData()
            .cron(new ConnectionScheduleDataCron().cronExpression(EXPECTED_CRON_EXPRESSION).cronTimeZone("Etc/foo"))));
    assertThrows(CronValidationInvalidExpressionProblem.class,
        () -> connectionScheduleHelper.populateSyncFromScheduleTypeAndData(actual, ConnectionScheduleType.CRON, new ConnectionScheduleData()
            .cron(new ConnectionScheduleDataCron().cronExpression("bad cron").cronTimeZone(EXPECTED_CRON_TIMEZONE))));
  }

  @Test
  void testAvailableCronTimeZonesStayTheSame() throws ConfigNotFoundException {
    /*
     * NOTE: this test exists to make sure that the server stays in sync with the frontend. The list of
     * supported timezones is copied from
     * oss/airbyte-webapp/src/core/utils/cron/availableCronTimeZones.json If this test fails, then THAT
     * file must be updated with the new timezones.
     */
    final String[] timezoneStrings = {
      "Africa/Abidjan",
      "Africa/Accra",
      "Africa/Addis_Ababa",
      "Africa/Algiers",
      "Africa/Asmara",
      "Africa/Asmera",
      "Africa/Bamako",
      "Africa/Bangui",
      "Africa/Banjul",
      "Africa/Bissau",
      "Africa/Blantyre",
      "Africa/Brazzaville",
      "Africa/Bujumbura",
      "Africa/Cairo",
      "Africa/Casablanca",
      "Africa/Ceuta",
      "Africa/Conakry",
      "Africa/Dakar",
      "Africa/Dar_es_Salaam",
      "Africa/Djibouti",
      "Africa/Douala",
      "Africa/El_Aaiun",
      "Africa/Freetown",
      "Africa/Gaborone",
      "Africa/Harare",
      "Africa/Johannesburg",
      "Africa/Juba",
      "Africa/Kampala",
      "Africa/Khartoum",
      "Africa/Kigali",
      "Africa/Kinshasa",
      "Africa/Lagos",
      "Africa/Libreville",
      "Africa/Lome",
      "Africa/Luanda",
      "Africa/Lubumbashi",
      "Africa/Lusaka",
      "Africa/Malabo",
      "Africa/Maputo",
      "Africa/Maseru",
      "Africa/Mbabane",
      "Africa/Mogadishu",
      "Africa/Monrovia",
      "Africa/Nairobi",
      "Africa/Ndjamena",
      "Africa/Niamey",
      "Africa/Nouakchott",
      "Africa/Ouagadougou",
      "Africa/Porto-Novo",
      "Africa/Sao_Tome",
      "Africa/Timbuktu",
      "Africa/Tripoli",
      "Africa/Tunis",
      "Africa/Windhoek",
      "America/Adak",
      "America/Anchorage",
      "America/Anguilla",
      "America/Antigua",
      "America/Araguaina",
      "America/Argentina/Buenos_Aires",
      "America/Argentina/Catamarca",
      "America/Argentina/ComodRivadavia",
      "America/Argentina/Cordoba",
      "America/Argentina/Jujuy",
      "America/Argentina/La_Rioja",
      "America/Argentina/Mendoza",
      "America/Argentina/Rio_Gallegos",
      "America/Argentina/Salta",
      "America/Argentina/San_Juan",
      "America/Argentina/San_Luis",
      "America/Argentina/Tucuman",
      "America/Argentina/Ushuaia",
      "America/Aruba",
      "America/Asuncion",
      "America/Atikokan",
      "America/Atka",
      "America/Bahia",
      "America/Bahia_Banderas",
      "America/Barbados",
      "America/Belem",
      "America/Belize",
      "America/Blanc-Sablon",
      "America/Boa_Vista",
      "America/Bogota",
      "America/Boise",
      "America/Buenos_Aires",
      "America/Cambridge_Bay",
      "America/Campo_Grande",
      "America/Cancun",
      "America/Caracas",
      "America/Catamarca",
      "America/Cayenne",
      "America/Cayman",
      "America/Chicago",
      "America/Chihuahua",
      "America/Coral_Harbour",
      "America/Cordoba",
      "America/Costa_Rica",
      "America/Creston",
      "America/Cuiaba",
      "America/Curacao",
      "America/Danmarkshavn",
      "America/Dawson",
      "America/Dawson_Creek",
      "America/Denver",
      "America/Detroit",
      "America/Dominica",
      "America/Edmonton",
      "America/Eirunepe",
      "America/El_Salvador",
      "America/Ensenada",
      "America/Fort_Nelson",
      "America/Fort_Wayne",
      "America/Fortaleza",
      "America/Glace_Bay",
      "America/Godthab",
      "America/Goose_Bay",
      "America/Grand_Turk",
      "America/Grenada",
      "America/Guadeloupe",
      "America/Guatemala",
      "America/Guayaquil",
      "America/Guyana",
      "America/Halifax",
      "America/Havana",
      "America/Hermosillo",
      "America/Indiana/Indianapolis",
      "America/Indiana/Knox",
      "America/Indiana/Marengo",
      "America/Indiana/Petersburg",
      "America/Indiana/Tell_City",
      "America/Indiana/Vevay",
      "America/Indiana/Vincennes",
      "America/Indiana/Winamac",
      "America/Indianapolis",
      "America/Inuvik",
      "America/Iqaluit",
      "America/Jamaica",
      "America/Jujuy",
      "America/Juneau",
      "America/Kentucky/Louisville",
      "America/Kentucky/Monticello",
      "America/Knox_IN",
      "America/Kralendijk",
      "America/La_Paz",
      "America/Lima",
      "America/Los_Angeles",
      "America/Louisville",
      "America/Lower_Princes",
      "America/Maceio",
      "America/Managua",
      "America/Manaus",
      "America/Marigot",
      "America/Martinique",
      "America/Matamoros",
      "America/Mazatlan",
      "America/Mendoza",
      "America/Menominee",
      "America/Merida",
      "America/Metlakatla",
      "America/Mexico_City",
      "America/Miquelon",
      "America/Moncton",
      "America/Monterrey",
      "America/Montevideo",
      "America/Montreal",
      "America/Montserrat",
      "America/Nassau",
      "America/New_York",
      "America/Nipigon",
      "America/Nome",
      "America/Noronha",
      "America/North_Dakota/Beulah",
      "America/North_Dakota/Center",
      "America/North_Dakota/New_Salem",
      "America/Ojinaga",
      "America/Panama",
      "America/Pangnirtung",
      "America/Paramaribo",
      "America/Phoenix",
      "America/Port-au-Prince",
      "America/Port_of_Spain",
      "America/Porto_Acre",
      "America/Porto_Velho",
      "America/Puerto_Rico",
      "America/Punta_Arenas",
      "America/Rainy_River",
      "America/Rankin_Inlet",
      "America/Recife",
      "America/Regina",
      "America/Resolute",
      "America/Rio_Branco",
      "America/Rosario",
      "America/Santa_Isabel",
      "America/Santarem",
      "America/Santiago",
      "America/Santo_Domingo",
      "America/Sao_Paulo",
      "America/Scoresbysund",
      "America/Shiprock",
      "America/Sitka",
      "America/St_Barthelemy",
      "America/St_Johns",
      "America/St_Kitts",
      "America/St_Lucia",
      "America/St_Thomas",
      "America/St_Vincent",
      "America/Swift_Current",
      "America/Tegucigalpa",
      "America/Thule",
      "America/Thunder_Bay",
      "America/Tijuana",
      "America/Toronto",
      "America/Tortola",
      "America/Vancouver",
      "America/Virgin",
      "America/Whitehorse",
      "America/Winnipeg",
      "America/Yakutat",
      "America/Yellowknife",
      "Antarctica/Casey",
      "Antarctica/Davis",
      "Antarctica/DumontDUrville",
      "Antarctica/Macquarie",
      "Antarctica/Mawson",
      "Antarctica/McMurdo",
      "Antarctica/Palmer",
      "Antarctica/Rothera",
      "Antarctica/South_Pole",
      "Antarctica/Syowa",
      "Antarctica/Troll",
      "Antarctica/Vostok",
      "Arctic/Longyearbyen",
      "Asia/Aden",
      "Asia/Almaty",
      "Asia/Amman",
      "Asia/Anadyr",
      "Asia/Aqtau",
      "Asia/Aqtobe",
      "Asia/Ashgabat",
      "Asia/Ashkhabad",
      "Asia/Atyrau",
      "Asia/Baghdad",
      "Asia/Bahrain",
      "Asia/Baku",
      "Asia/Bangkok",
      "Asia/Barnaul",
      "Asia/Beirut",
      "Asia/Bishkek",
      "Asia/Brunei",
      "Asia/Calcutta",
      "Asia/Chita",
      "Asia/Choibalsan",
      "Asia/Chongqing",
      "Asia/Chungking",
      "Asia/Colombo",
      "Asia/Dacca",
      "Asia/Damascus",
      "Asia/Dhaka",
      "Asia/Dili",
      "Asia/Dubai",
      "Asia/Dushanbe",
      "Asia/Famagusta",
      "Asia/Gaza",
      "Asia/Harbin",
      "Asia/Hebron",
      "Asia/Ho_Chi_Minh",
      "Asia/Hong_Kong",
      "Asia/Hovd",
      "Asia/Irkutsk",
      "Asia/Istanbul",
      "Asia/Jakarta",
      "Asia/Jayapura",
      "Asia/Jerusalem",
      "Asia/Kabul",
      "Asia/Kamchatka",
      "Asia/Karachi",
      "Asia/Kashgar",
      "Asia/Kathmandu",
      "Asia/Katmandu",
      "Asia/Khandyga",
      "Asia/Kolkata",
      "Asia/Krasnoyarsk",
      "Asia/Kuala_Lumpur",
      "Asia/Kuching",
      "Asia/Kuwait",
      "Asia/Macao",
      "Asia/Macau",
      "Asia/Magadan",
      "Asia/Makassar",
      "Asia/Manila",
      "Asia/Muscat",
      "Asia/Nicosia",
      "Asia/Novokuznetsk",
      "Asia/Novosibirsk",
      "Asia/Omsk",
      "Asia/Oral",
      "Asia/Phnom_Penh",
      "Asia/Pontianak",
      "Asia/Pyongyang",
      "Asia/Qatar",
      "Asia/Qostanay",
      "Asia/Qyzylorda",
      "Asia/Rangoon",
      "Asia/Riyadh",
      "Asia/Saigon",
      "Asia/Sakhalin",
      "Asia/Samarkand",
      "Asia/Seoul",
      "Asia/Shanghai",
      "Asia/Singapore",
      "Asia/Srednekolymsk",
      "Asia/Taipei",
      "Asia/Tashkent",
      "Asia/Tbilisi",
      "Asia/Tehran",
      "Asia/Tel_Aviv",
      "Asia/Thimbu",
      "Asia/Thimphu",
      "Asia/Tokyo",
      "Asia/Tomsk",
      "Asia/Ujung_Pandang",
      "Asia/Ulaanbaatar",
      "Asia/Ulan_Bator",
      "Asia/Urumqi",
      "Asia/Ust-Nera",
      "Asia/Vientiane",
      "Asia/Vladivostok",
      "Asia/Yakutsk",
      "Asia/Yangon",
      "Asia/Yekaterinburg",
      "Asia/Yerevan",
      "Atlantic/Azores",
      "Atlantic/Bermuda",
      "Atlantic/Canary",
      "Atlantic/Cape_Verde",
      "Atlantic/Faeroe",
      "Atlantic/Faroe",
      "Atlantic/Jan_Mayen",
      "Atlantic/Madeira",
      "Atlantic/Reykjavik",
      "Atlantic/South_Georgia",
      "Atlantic/St_Helena",
      "Atlantic/Stanley",
      "Australia/ACT",
      "Australia/Adelaide",
      "Australia/Brisbane",
      "Australia/Broken_Hill",
      "Australia/Canberra",
      "Australia/Currie",
      "Australia/Darwin",
      "Australia/Eucla",
      "Australia/Hobart",
      "Australia/LHI",
      "Australia/Lindeman",
      "Australia/Lord_Howe",
      "Australia/Melbourne",
      "Australia/NSW",
      "Australia/North",
      "Australia/Perth",
      "Australia/Queensland",
      "Australia/South",
      "Australia/Sydney",
      "Australia/Tasmania",
      "Australia/Victoria",
      "Australia/West",
      "Australia/Yancowinna",
      "Brazil/Acre",
      "Brazil/DeNoronha",
      "Brazil/East",
      "Brazil/West",
      "CET",
      "CST6CDT",
      "Canada/Atlantic",
      "Canada/Central",
      "Canada/Eastern",
      "Canada/Mountain",
      "Canada/Newfoundland",
      "Canada/Pacific",
      "Canada/Saskatchewan",
      "Canada/Yukon",
      "Chile/Continental",
      "Chile/EasterIsland",
      "Cuba",
      "EET",
      "EST",
      "EST5EDT",
      "Egypt",
      "Eire",
      "Europe/Amsterdam",
      "Europe/Andorra",
      "Europe/Astrakhan",
      "Europe/Athens",
      "Europe/Belfast",
      "Europe/Belgrade",
      "Europe/Berlin",
      "Europe/Bratislava",
      "Europe/Brussels",
      "Europe/Bucharest",
      "Europe/Budapest",
      "Europe/Busingen",
      "Europe/Chisinau",
      "Europe/Copenhagen",
      "Europe/Dublin",
      "Europe/Gibraltar",
      "Europe/Guernsey",
      "Europe/Helsinki",
      "Europe/Isle_of_Man",
      "Europe/Istanbul",
      "Europe/Jersey",
      "Europe/Kaliningrad",
      "Europe/Kiev",
      "Europe/Kirov",
      "Europe/Lisbon",
      "Europe/Ljubljana",
      "Europe/London",
      "Europe/Luxembourg",
      "Europe/Madrid",
      "Europe/Malta",
      "Europe/Mariehamn",
      "Europe/Minsk",
      "Europe/Monaco",
      "Europe/Moscow",
      "Europe/Nicosia",
      "Europe/Oslo",
      "Europe/Paris",
      "Europe/Podgorica",
      "Europe/Prague",
      "Europe/Riga",
      "Europe/Rome",
      "Europe/Samara",
      "Europe/San_Marino",
      "Europe/Sarajevo",
      "Europe/Saratov",
      "Europe/Simferopol",
      "Europe/Skopje",
      "Europe/Sofia",
      "Europe/Stockholm",
      "Europe/Tallinn",
      "Europe/Tirane",
      "Europe/Tiraspol",
      "Europe/Ulyanovsk",
      "Europe/Uzhgorod",
      "Europe/Vaduz",
      "Europe/Vatican",
      "Europe/Vienna",
      "Europe/Vilnius",
      "Europe/Volgograd",
      "Europe/Warsaw",
      "Europe/Zagreb",
      "Europe/Zaporozhye",
      "Europe/Zurich",
      "GB",
      "GB-Eire",
      "GMT",
      "GMT+0",
      "GMT-0",
      "GMT0",
      "Greenwich",
      "HST",
      "Hongkong",
      "Iceland",
      "Indian/Antananarivo",
      "Indian/Chagos",
      "Indian/Christmas",
      "Indian/Cocos",
      "Indian/Comoro",
      "Indian/Kerguelen",
      "Indian/Mahe",
      "Indian/Maldives",
      "Indian/Mauritius",
      "Indian/Mayotte",
      "Indian/Reunion",
      "Iran",
      "Israel",
      "Jamaica",
      "Japan",
      "Kwajalein",
      "Libya",
      "MET",
      "MST",
      "MST7MDT",
      "Mexico/BajaNorte",
      "Mexico/BajaSur",
      "Mexico/General",
      "NZ",
      "NZ-CHAT",
      "Navajo",
      "PRC",
      "PST8PDT",
      "Pacific/Apia",
      "Pacific/Auckland",
      "Pacific/Bougainville",
      "Pacific/Chatham",
      "Pacific/Chuuk",
      "Pacific/Easter",
      "Pacific/Efate",
      "Pacific/Enderbury",
      "Pacific/Fakaofo",
      "Pacific/Fiji",
      "Pacific/Funafuti",
      "Pacific/Galapagos",
      "Pacific/Gambier",
      "Pacific/Guadalcanal",
      "Pacific/Guam",
      "Pacific/Honolulu",
      "Pacific/Johnston",
      "Pacific/Kiritimati",
      "Pacific/Kosrae",
      "Pacific/Kwajalein",
      "Pacific/Majuro",
      "Pacific/Marquesas",
      "Pacific/Midway",
      "Pacific/Nauru",
      "Pacific/Niue",
      "Pacific/Norfolk",
      "Pacific/Noumea",
      "Pacific/Pago_Pago",
      "Pacific/Palau",
      "Pacific/Pitcairn",
      "Pacific/Pohnpei",
      "Pacific/Ponape",
      "Pacific/Port_Moresby",
      "Pacific/Rarotonga",
      "Pacific/Saipan",
      "Pacific/Samoa",
      "Pacific/Tahiti",
      "Pacific/Tarawa",
      "Pacific/Tongatapu",
      "Pacific/Truk",
      "Pacific/Wake",
      "Pacific/Wallis",
      "Pacific/Yap",
      "Poland",
      "Portugal",
      "ROC",
      "ROK",
      "Singapore",
      "Turkey",
      "UCT",
      "US/Alaska",
      "US/Aleutian",
      "US/Arizona",
      "US/Central",
      "US/East-Indiana",
      "US/Eastern",
      "US/Hawaii",
      "US/Indiana-Starke",
      "US/Michigan",
      "US/Mountain",
      "US/Pacific",
      "US/Samoa",
      "UTC",
      "Universal",
      "W-SU",
      "WET",
      "Zulu"
    };
    for (final String expectedTimezone : timezoneStrings) {
      try {
        final StandardSync actual = new StandardSync();
        // NOTE: this method call is the one that parses the given timezone string
        // and will throw an exception if it isn't supported. This method is called
        // on the API handler path.
        connectionScheduleHelper.populateSyncFromScheduleTypeAndData(actual,
            ConnectionScheduleType.CRON, new ConnectionScheduleData()
                .cron(new ConnectionScheduleDataCron()
                    .cronTimeZone(expectedTimezone)
                    .cronExpression(EXPECTED_CRON_EXPRESSION)));
        assertEquals(ScheduleType.CRON, actual.getScheduleType());
        assertEquals(expectedTimezone, actual.getScheduleData().getCron().getCronTimeZone());
        assertEquals(EXPECTED_CRON_EXPRESSION, actual.getScheduleData().getCron().getCronExpression());
      } catch (final IllegalArgumentException | JsonValidationException e) {
        throw (RuntimeException) new RuntimeException(
            "One of the timezones is not supported - update oss/airbyte-webapp/src/components/connection/ConnectionForm/availableCronTimeZones.json!")
                .initCause(e);
      }
    }
  }

}
