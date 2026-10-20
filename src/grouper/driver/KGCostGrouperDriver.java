// SPDX-License-Identifier: CC0-1.0
// KG-COST MS-DRG grouper subprocess driver.
//
// Reads newline-delimited JSON requests from stdin, dispatches each to the
// per-version MS-DRG grouper API via reflection on
// gov.agency.msdrg.v{maj}{min}.MsdrgComponent, and writes newline-delimited
// JSON responses to stdout. JVM stays alive across requests; per-version
// MsdrgComponent is constructed once and reused (per pbl161_msdrg_java_api.pdf
// p.7 -- nulling the component in batch is a known performance pitfall).
//
// Each request MUST carry a "version" field, e.g. "V43.1" or "V25.0". The
// driver resolves it to the entry-point class via Class.forName, then casts to
// the shared MsdrgComponentApi interface (which the model JAR does not
// publish). Construction + process() are reflective; everything else (input,
// output, enums) lives in the shared model JAR (msdrg-model-v2-*.jar) and is
// referenced statically.
//
// Hand-rolled flat JSON parser/serializer -- no external JSON library is
// shipped with the cms.gov standalone JARs ZIP. Sufficient for the fixed
// request/response schema in plans/plan-v9.md Step 4.

import com.mmm.his.cer.foundation.model.GfcPoa;
import gov.agency.msdrg.model.v2.*;
import gov.agency.msdrg.model.v2.enumeration.*;
import gov.agency.msdrg.model.v2.transfer.MsdrgClaim;
import gov.agency.msdrg.model.v2.transfer.input.*;
import gov.agency.msdrg.model.v2.transfer.output.MsdrgOutputData;
import java.io.*;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class KGCostGrouperDriver {

    /** version string ("V43.1") -> ready-to-use grouper component. */
    private static final Map<String, Object> COMPONENTS = new HashMap<>();
    /** version string -> resolved process(MsdrgClaim) method handle. */
    private static final Map<String, Method> PROCESS_METHODS = new HashMap<>();

    public static void main(String[] args) {
        PrintStream out = new PrintStream(System.out, true, StandardCharsets.UTF_8);
        out.println("READY");
        BufferedReader in = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
        String line;
        try {
            while ((line = in.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) continue;
                Map<String, Object> req;
                try { req = Json.parseObject(line); }
                catch (Exception e) { out.println(serialize(Map.of("error", "json_parse: " + safe(e.getMessage())))); continue; }
                Map<String, Object> resp;
                try { resp = handle(req); }
                catch (Throwable t) { resp = Map.of("error", t.getClass().getSimpleName() + ": " + safe(t.getMessage())); }
                out.println(serialize(resp));
            }
        } catch (Throwable t) {
            out.println(serialize(Map.of("fatal", "loop_failed: " + safe(t.getMessage()))));
            System.exit(3); return;
        }
        System.exit(0);
    }

    /** Parse "V43.1" / "V25.0" -> {"43", "1"} / {"25", "0"}. */
    private static String[] parseMajorMinor(String version) {
        if (version == null || version.isEmpty()) {
            throw new IllegalArgumentException("version required (e.g. \"V43.1\")");
        }
        String bare = version.startsWith("V") || version.startsWith("v")
            ? version.substring(1) : version;
        int dot = bare.indexOf('.');
        if (dot < 0 || dot == bare.length() - 1) {
            throw new IllegalArgumentException("malformed version: " + version);
        }
        return new String[] { bare.substring(0, dot), bare.substring(dot + 1) };
    }

    /** "V43.1" -> "gov.agency.msdrg.v431.MsdrgComponent" (per CLAUDE.md Phase 4 landmine). */
    private static String entryPointClass(String version) {
        String[] mm = parseMajorMinor(version);
        return "gov.agency.msdrg.v" + mm[0] + mm[1] + ".MsdrgComponent";
    }

    /** Get-or-construct the MsdrgComponent for `version`, cached for the lifetime of the JVM. */
    private static Object getComponent(String version) throws Exception {
        Object c = COMPONENTS.get(version);
        if (c != null) return c;
        String fqcn = entryPointClass(version);
        Class<?> cls;
        try {
            cls = Class.forName(fqcn);
        } catch (ClassNotFoundException cnfe) {
            throw new IllegalStateException(
                "version " + version + " not on classpath (expected class " + fqcn + "); "
                + "vendor it via `python3 -m src.extract.grouper --fy <fy> --version " + version + "`");
        }
        MsdrgRuntimeOption rto = new MsdrgRuntimeOption();
        RuntimeOptions opts = new RuntimeOptions();
        opts.setPoaReportingExempt(MsdrgHospitalStatusOptionFlag.NON_EXEMPT);
        opts.setComputeAffectDrg(MsdrgAffectDrgOptionFlag.COMPUTE);
        opts.setMarkingLogicTieBreaker(MarkingLogicTieBreaker.CLINICAL_SIGNIFICANCE);
        rto.put(MsdrgOption.RUNTIME_OPTION_FLAGS, opts);
        Constructor<?> ctor = cls.getConstructor(MsdrgRuntimeOption.class);
        c = ctor.newInstance(rto);
        Method process = cls.getMethod("process", MsdrgClaim.class);
        COMPONENTS.put(version, c);
        PROCESS_METHODS.put(version, process);
        return c;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> handle(Map<String, Object> req) throws Exception {
        String version = str(req, "version", null);
        if (version == null || version.isEmpty()) {
            throw new IllegalArgumentException("version field required (e.g. \"V43.1\")");
        }
        Object component = getComponent(version);
        Method process = PROCESS_METHODS.get(version);

        List<Object> dxRaw = (List<Object>) req.get("diagnoses");
        if (dxRaw == null || dxRaw.isEmpty()) throw new IllegalArgumentException("diagnoses required");
        Map<String, Object> pdxObj = (Map<String, Object>) dxRaw.get(0);
        String pdx = str(pdxObj, "code", null);
        if (pdx == null || pdx.isEmpty()) throw new IllegalArgumentException("pdx required");
        GfcPoa pdxPoa = poaFromString(str(pdxObj, "poa", "Y"));

        List<Object> pcsRaw = (List<Object>) req.getOrDefault("pcs", new ArrayList<Object>());
        List<MsdrgInputDxCode> sdxList = new ArrayList<>(dxRaw.size() - 1);
        for (int i = 1; i < dxRaw.size(); i++) {
            Map<String, Object> dx = (Map<String, Object>) dxRaw.get(i);
            sdxList.add(new MsdrgInputDxCode(str(dx, "code", ""), poaFromString(str(dx, "poa", "Y"))));
        }
        List<MsdrgInputPrCode> pcsList = new ArrayList<>(pcsRaw.size());
        for (Object o : pcsRaw) pcsList.add(new MsdrgInputPrCode(String.valueOf(o)));

        MsdrgInput input = MsdrgInput.builder()
            .withPrincipalDiagnosisCode(new MsdrgInputDxCode(pdx, pdxPoa))
            .withSecondaryDiagnosisCodes(sdxList)
            .withProcedureCodes(pcsList)
            .withSex(sexFromCode(str(req, "sex", "U")))
            .withAgeInYears(intOf(req, "age", 0))
            .withAgeDaysAdmit(intOf(req, "age_days_admit", 0))
            .withAgeDaysDischarge(intOf(req, "age_days_discharge", 0))
            .withDischargeStatus(dischargeFromCode(str(req, "discharge_disp", "01")))
            .build();

        MsdrgClaim claim = new MsdrgClaim(input);
        process.invoke(component, claim);
        Optional<MsdrgOutputData> outOpt = claim.getOutput();
        if (outOpt.isEmpty()) throw new IllegalStateException("grouper returned no output");
        MsdrgOutputData od = outOpt.get();

        Map<String, Object> r = new LinkedHashMap<>();
        r.put("ms_drg", drg3(od.getFinalDrg())); r.put("mdc", mdc2(od.getFinalMdc()));
        r.put("severity_tier", sev(od.getFinalSeverity()));
        r.put("or_procedure_flag", od.getFinalMedSugType() == MsdrgMedSurgType.SURGICAL);
        r.put("initial_drg", drg3(od.getInitialDrg())); r.put("initial_mdc", mdc2(od.getInitialMdc()));
        r.put("initial_severity", sev(od.getInitialSeverity()));
        r.put("final_grc", od.getFinalGrc() == null ? "NONE" : od.getFinalGrc().toString());
        r.put("initial_grc", od.getInitialGrc() == null ? "NONE" : od.getInitialGrc().toString());
        r.put("hac_status", od.getHacStatus() == null ? "NONE" : od.getHacStatus().toString());
        r.put("num_hac_categories", od.getNumHacCategoriesSatisfied());
        r.put("final_base_drg", drg3(od.getFinalBaseDrg()));
        r.put("initial_base_drg", drg3(od.getInitialBaseDrg()));
        return r;
    }

    /** CMS POA char -> GfcPoa via fromChar (Y/N/U/W/1/E + BLANK fallback). */
    private static GfcPoa poaFromString(String s) {
        if (s == null || s.isEmpty()) return GfcPoa.Y;
        return GfcPoa.fromChar(Character.toUpperCase(s.charAt(0)), GfcPoa.Y);
    }

    private static MsdrgSex sexFromCode(String s) {
        if (s == null) return MsdrgSex.UNKNOWN;
        switch (s.toUpperCase()) {
            case "M": case "MALE": case "1": return MsdrgSex.MALE;
            case "F": case "FEMALE": case "2": return MsdrgSex.FEMALE;
            default: return MsdrgSex.UNKNOWN;
        }
    }

    /** CMS UB-04 2-digit code -> MsdrgDischargeStatus. Unknown -> HOME_SELFCARE_ROUTINE. */
    private static MsdrgDischargeStatus dischargeFromCode(String s) {
        if (s == null || s.isEmpty()) return MsdrgDischargeStatus.HOME_SELFCARE_ROUTINE;
        try {
            MsdrgDischargeStatus ds = MsdrgDischargeStatus.getEnumFromInt(Integer.parseInt(s.trim()));
            return ds == null ? MsdrgDischargeStatus.HOME_SELFCARE_ROUTINE : ds;
        } catch (NumberFormatException nfe) { return MsdrgDischargeStatus.HOME_SELFCARE_ROUTINE; }
    }

    private static String sev(MsdrgSeverity sv) {
        if (sv == null) return "none";
        switch (sv) { case MCC: return "with_mcc"; case CC: return "with_cc"; case NON_CC: return "without_cc_mcc"; default: return "none"; }
    }
    private static String drg3(MsdrgValue<Integer> v) { return (v == null || v.getValue() == null) ? "000" : String.format("%03d", v.getValue()); }
    private static String mdc2(MsdrgValue<Integer> v) { return (v == null || v.getValue() == null) ? "00" : String.format("%02d", v.getValue()); }
    private static String str(Map<String, Object> m, String k, String dflt) { Object v = m.get(k); return v == null ? dflt : String.valueOf(v); }
    private static int intOf(Map<String, Object> m, String k, int dflt) {
        Object v = m.get(k);
        if (v == null) return dflt;
        if (v instanceof Number) return ((Number) v).intValue();
        try { return Integer.parseInt(String.valueOf(v).trim()); } catch (Exception e) { return dflt; }
    }
    private static String safe(String s) { return s == null ? "" : s.replace('"', '\''); }

    // ---- Hand-rolled JSON (flat objects only) ----

    private static String serialize(Map<String, Object> m) {
        StringBuilder sb = new StringBuilder(256).append('{');
        boolean first = true;
        for (Map.Entry<String, Object> e : m.entrySet()) {
            if (!first) sb.append(','); first = false;
            sb.append('"').append(esc(e.getKey())).append("\":"); val(sb, e.getValue());
        }
        return sb.append('}').toString();
    }

    private static void val(StringBuilder sb, Object v) {
        if (v == null) sb.append("null");
        else if (v instanceof Boolean) sb.append(((Boolean) v) ? "true" : "false");
        else if (v instanceof Number) sb.append(v.toString());
        else if (v instanceof List) {
            sb.append('['); boolean f = true;
            for (Object x : (List<?>) v) { if (!f) sb.append(','); f = false; val(sb, x); }
            sb.append(']');
        } else sb.append('"').append(esc(String.valueOf(v))).append('"');
    }

    private static String esc(String s) {
        StringBuilder sb = new StringBuilder(s.length() + 4);
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '"' || c == '\\') sb.append('\\').append(c);
            else if (c == '\n') sb.append("\\n");
            else if (c == '\r') sb.append("\\r");
            else if (c == '\t') sb.append("\\t");
            else if (c < 0x20) sb.append(String.format("\\u%04x", (int) c));
            else sb.append(c);
        }
        return sb.toString();
    }

    private static final class Json {
        private final String s; private int i;
        private Json(String s) { this.s = s; }

        @SuppressWarnings("unchecked")
        static Map<String, Object> parseObject(String s) {
            Json p = new Json(s); p.skipWs();
            Object o = p.parseValue();
            if (!(o instanceof Map)) throw new IllegalArgumentException("top-level must be object");
            return (Map<String, Object>) o;
        }

        private Object parseValue() {
            skipWs();
            if (i >= s.length()) throw new IllegalArgumentException("eof");
            char c = s.charAt(i);
            if (c == '{') return parseObj();
            if (c == '[') return parseArr();
            if (c == '"') return parseStr();
            if (c == 't' || c == 'f') return parseBool();
            if (c == 'n') { expect("null"); return null; }
            return parseNum();
        }

        private Map<String, Object> parseObj() {
            Map<String, Object> m = new HashMap<>(); i++; skipWs();
            if (peek() == '}') { i++; return m; }
            while (true) {
                skipWs(); String key = parseStr(); skipWs();
                if (peek() != ':') throw new IllegalArgumentException("expected ':' at " + i);
                i++; m.put(key, parseValue()); skipWs();
                char c = peek(); i++;
                if (c == '}') return m;
                if (c != ',') throw new IllegalArgumentException("expected ',' or '}' at " + i);
            }
        }

        private List<Object> parseArr() {
            List<Object> a = new ArrayList<>(); i++; skipWs();
            if (peek() == ']') { i++; return a; }
            while (true) {
                a.add(parseValue()); skipWs();
                char c = peek(); i++;
                if (c == ']') return a;
                if (c != ',') throw new IllegalArgumentException("expected ',' or ']' at " + i);
            }
        }

        private String parseStr() {
            if (s.charAt(i) != '"') throw new IllegalArgumentException("expected '\"' at " + i);
            i++; StringBuilder sb = new StringBuilder();
            while (i < s.length()) {
                char c = s.charAt(i++);
                if (c == '"') return sb.toString();
                if (c != '\\') { sb.append(c); continue; }
                char e = s.charAt(i++);
                if (e == 'n') sb.append('\n'); else if (e == 'r') sb.append('\r');
                else if (e == 't') sb.append('\t');
                else if (e == 'u') { sb.append((char) Integer.parseInt(s.substring(i, i + 4), 16)); i += 4; }
                else sb.append(e);  // " \ / and any other char passes through literally
            }
            throw new IllegalArgumentException("unterminated string");
        }

        private Object parseNum() {
            int start = i;
            if (peek() == '-') i++;
            while (i < s.length() && "0123456789.eE+-".indexOf(s.charAt(i)) >= 0) i++;
            String t = s.substring(start, i);
            if (t.indexOf('.') >= 0 || t.indexOf('e') >= 0 || t.indexOf('E') >= 0) return Double.parseDouble(t);
            try { return Integer.parseInt(t); } catch (NumberFormatException nfe) { return Long.parseLong(t); }
        }

        private Boolean parseBool() {
            if (s.charAt(i) == 't') { expect("true"); return Boolean.TRUE; }
            expect("false"); return Boolean.FALSE;
        }
        private void expect(String t) { if (!s.startsWith(t, i)) throw new IllegalArgumentException("expected " + t); i += t.length(); }
        private void skipWs() { while (i < s.length() && Character.isWhitespace(s.charAt(i))) i++; }
        private char peek() { if (i >= s.length()) throw new IllegalArgumentException("eof"); return s.charAt(i); }
    }
}
