/*
 * CloudSim Plus: A modern, highly-extensible and easier-to-use Framework for
 * Modeling and Simulation of Cloud Computing Infrastructures and Services.
 * http://cloudsimplus.org
 *
 *     Copyright (C) 2015-2018 Universidade da Beira Interior (UBI, Portugal) and
 *     the Instituto Federal de Educação Ciência e Tecnologia do Tocantins (IFTO, Brazil).
 *
 *     This file is part of CloudSim Plus.
 *
 *     CloudSim Plus is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU General Public License as published by
 *     the Free Software Foundation, either version 3 of the License, or
 *     (at your option) any later version.
 *
 *     CloudSim Plus is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU General Public License for more details.
 *
 *     You should have received a copy of the GNU General Public License
 *     along with CloudSim Plus. If not, see <http://www.gnu.org/licenses/>.
 */
package org.cloudsimplus.Implementation.autoscaling;

import org.cloudbus.cloudsim.allocationpolicies.VmAllocationPolicySimple;
import org.cloudbus.cloudsim.brokers.DatacenterBroker;
import org.cloudbus.cloudsim.brokers.DatacenterBrokerSimple;
import org.cloudbus.cloudsim.core.Simulation;
import org.cloudbus.cloudsim.distributions.ContinuousDistribution;
import org.cloudbus.cloudsim.distributions.UniformDistr;
import org.cloudbus.cloudsim.schedulers.cloudlet.CloudletSchedulerTimeShared;
import org.cloudsimplus.autoscaling.HorizontalVmScaling;
import org.cloudsimplus.autoscaling.HorizontalVmScalingSimple;
import org.cloudbus.cloudsim.cloudlets.Cloudlet;
import org.cloudbus.cloudsim.cloudlets.CloudletSimple;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.datacenters.Datacenter;
import org.cloudbus.cloudsim.datacenters.DatacenterCharacteristics;
import org.cloudbus.cloudsim.datacenters.DatacenterCharacteristicsSimple;
import org.cloudbus.cloudsim.datacenters.DatacenterSimple;
import org.cloudbus.cloudsim.hosts.Host;
import org.cloudbus.cloudsim.hosts.HostSimple;
import org.cloudbus.cloudsim.provisioners.PeProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.ResourceProvisionerSimple;
import org.cloudbus.cloudsim.resources.Pe;
import org.cloudbus.cloudsim.resources.PeSimple;
import org.cloudbus.cloudsim.schedulers.vm.VmScheduler;
import org.cloudbus.cloudsim.schedulers.vm.VmSchedulerTimeShared;
/*import org.cloudbus.cloudsim.util.Log;*/
/*import org.cloudbus.cloudsim.util.WorkloadFileReader;*/
import org.cloudbus.cloudsim.utilizationmodels.UtilizationModel;
import org.cloudbus.cloudsim.utilizationmodels.UtilizationModelFull;
import org.cloudbus.cloudsim.vms.Vm;
import org.cloudbus.cloudsim.vms.VmSimple;
import org.cloudbus.cloudsim.vms.VmStateHistoryEntry;
import org.cloudsimplus.listeners.CloudletVmEventInfo;
import org.cloudsimplus.listeners.EventInfo;
import org.cloudsimplus.listeners.EventListener;
import org.cloudsimplus.listeners.VmHostEventInfo;
import org.cloudsimplus.builders.tables.CloudletsTableBuilder;
import  org.cloudbus.cloudsim.core.Machine;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;

import static java.util.Comparator.comparingDouble;

import java.io.IOException;
import java.io.FileWriter;


public class HorizontalVmScalingImplementation {

	FileWriter fileWriterVm = null;
	
	  final String FILE_HEADER1 = "VM ID,CPU ,VM RAM ,VM BW,VM Storage,VM PES,StartTime,FinishTime SEC,ExecTime SEC";
		   //   ID,         ,  Seconds,   Seconds, Seconds;
	    final String COMMA_DELIMITER = ",";
	    final String NEW_LINE_SEPARATOR = "\n";
	
	private static final String WORKLOAD_FILENAME = "NASA-iPSC-1993-3.1-cln.swf.zip";
	private int maximumNumberOfCloudletsToCreateFromTheWorkloadFile = -1;
	
    
    
    List<Cloudlet> finishedCloudlets;
    
    private static final int NUMBER_OF_VMS_PER_HOST = 2;
    private static final int VM_PES = 2;
    private static final int VM_MIPS = 100;
    private static final long VM_SIZE = 2000;
    private static final int VM_RAM = 1000;
    private static final long VM_BW = 500;
    
   
    private static final int HOSTS = 2;
    private static final int HOST_PES = NUMBER_OF_VMS_PER_HOST*VM_PES;
    private static final int VMS = 2;
    
    private static final int CLOUDLETS = 6;
    private static final int CLOUDLETS_MIPS = 1000;
	private static final int SCHEDULING_INTERVAL=2;
    private static  double CLOUDLETS_CREATION_INTERVAL = 5;
    private static  int NoOFCLOUDLETS = 30;
    private static double CLOUDLETS_CREATION_TIMER=0;
    
    
    private static final double RAM_COST_PER_SEC=0.05;
    private static final double BW_COST_PER_SEC=0.05;
    private static final double STORAGE_COST_PER_SEC=0.05;
    private static final double DC_COST_PER_SEC=0.05;
    
    private final CloudSim simulation;
    private DatacenterBroker broker0;
    private List<Host> hostList;
    private List<Vm> vmList;
    private List<Cloudlet> cloudletList;
    private static final long[] CLOUDLET_LENGTHS = {200, 4000, 1000, 16000, 12000, 3000, 2100};
    private ContinuousDistribution rand;

    private int createdCloudlets;
    private int createsVms; 
    private Datacenter datacenter;

    public static void main(String[] args) {
        new HorizontalVmScalingImplementation();
    }

    /**
     * Default constructor that builds the simulation scenario and starts the simulation.
     */
    public HorizontalVmScalingImplementation() {
        /*You can remove the seed parameter to get a dynamic one, based on current computer time.
        * With a dynamic seed you will get different results at each simulation run.*/
    	String fileName=System.getProperty("user.home")+"/VM.csv";
    	try
    	{
    		fileWriterVm = new FileWriter(fileName);
    		fileWriterVm.append(FILE_HEADER1.toString());
    	}
    	catch(IOException e)
    	{
    	
    	}
    	final long seed = 1;
        rand = new UniformDistr(0, CLOUDLET_LENGTHS.length, seed);
        hostList = new ArrayList<>(HOSTS);
        vmList = new ArrayList<>(VMS);
        cloudletList = new ArrayList<>(CLOUDLETS);

        simulation = new CloudSim();
        simulation.addOnClockTickListener(this::ClockTicklesner);

        createDatacenter();
        broker0 = new DatacenterBrokerSimple(simulation);
        broker0.setVmMapper(this::bestFitCloudletToVmMapper);	
        /*
         * Defines the Vm Destruction Delay Function as a lambda expression
         * so that the broker will wait 10 seconds before destroying an idle VM.
         * By commenting this line, no down scaling will be performed
         * and idle VMs will be destroyed just after all running Cloudlets
         * are finished and there is no waiting Cloudlet. */
       broker0.setVmDestructionDelayFunction(vm -> 2.0);

        vmList.addAll(createListOfScalableVms(VMS));

       createCloudletList();
        /*try {
        	 createCloudletsFromWorkloadFile();
        } catch (IOException e) {
            Log.printConcatLine(e.getMessage());
        
        }*/
        
        broker0.submitVmList(vmList);
        broker0.submitCloudletList(cloudletList);
       
       final double finishTime= simulation.start();
      try { 
    	  fileWriterVm.append(NEW_LINE_SEPARATOR);
       fileWriterVm.append(NEW_LINE_SEPARATOR);
     List<Vm> vmtest= broker0.getVmCreatedList();
     
       for(Vm vm:vmtest)
       {
    	   
    	   WriteVmInfo(vm);
       }
       
    	   
       
       fileWriterVm.flush();
       
       fileWriterVm.close();
       }
       catch(IOException e) {
    	   
       }
        printSimulationResults();
        //showCpuUtilizationForAllVms(finishTime);
    }

    private void printSimulationResults() {
         finishedCloudlets = broker0.getCloudletFinishedList();
        Comparator<Cloudlet> sortByVmId = comparingDouble(c -> c.getVm().getId());
        Comparator<Cloudlet> sortByStartTime = comparingDouble(c -> c.getExecStartTime());
        finishedCloudlets.sort(sortByVmId.thenComparing(sortByStartTime));
       String path=System.getProperty("user.home")+"/Result.csv";
        WritCsvFile(path);
        //List<Cloudlet> WCloudlets= broker0.getCloudletWaitingList();
       // new CloudletsTableBuilder(WCloudlets).build();
        new CloudletsTableBuilder(finishedCloudlets).build();
    }

    private void createCloudletList() {
        for (int i = 0; i < CLOUDLETS; i++) {
        	Cloudlet cloudlet=createCloudlet();
        	cloudletList.add(cloudlet);
        	//cloudlet.setVm(vmList.get(0));
        }
    }
    private void ClockTicklesner(EventInfo eventInfo) {
        final long time= (long)simulation.clock();
       int vmSize=vmList.size();
       int HostSize=hostList.size();
        if(vmSize%HostSize==0 && vmSize>2 )
        {
            Host host = createHost();
            datacenter.addHost(host);
            

        }/**/
       if (time >= CLOUDLETS_CREATION_TIMER && time <=  80) {
    	   CLOUDLETS_CREATION_TIMER=time+CLOUDLETS_CREATION_INTERVAL;
        	
        	final int numberOfCloudlets = NoOFCLOUDLETS;
           /* Log.printFormattedLine("\t#Creating %d Cloudlets at time %d.", numberOfCloudlets, time);*/
            List<Cloudlet> newCloudlets = new ArrayList<>(numberOfCloudlets);
            for (int i = 0; i < numberOfCloudlets; i++) {
                Cloudlet cloudlet = createCloudlet();
                cloudletList.add(cloudlet);
                newCloudlets.add(cloudlet);
            }

            broker0.submitCloudletList(newCloudlets);
        } /**/
    }

    private void createDatacenter() {
        for (int i = 0; i < HOSTS; i++) {
            hostList.add(createHost());
        }

         datacenter = new DatacenterSimple(simulation, hostList, new VmAllocationPolicySimple());
         datacenter.getCharacteristics()
         .setCostPerSecond(DC_COST_PER_SEC)
         .setCostPerMem(RAM_COST_PER_SEC)
         .setCostPerStorage(STORAGE_COST_PER_SEC)
         .setCostPerBw(BW_COST_PER_SEC);
     
        datacenter.setSchedulingInterval(SCHEDULING_INTERVAL);
    }

    private Host createHost() {
        List<Pe> peList = new ArrayList<>(HOST_PES);
        for (int i = 0; i < HOST_PES; i++) {
            peList.add(new PeSimple(VM_MIPS, new PeProvisionerSimple()));
        }

        final long ram = VM_RAM * NUMBER_OF_VMS_PER_HOST;
        final long storage = VM_SIZE * NUMBER_OF_VMS_PER_HOST;
        final long bw = VM_BW * NUMBER_OF_VMS_PER_HOST;
        return new HostSimple(ram, bw, storage, peList)
            .setRamProvisioner(new ResourceProvisionerSimple())
            .setBwProvisioner(new ResourceProvisionerSimple())
            .setVmScheduler(new VmSchedulerTimeShared());
    }

    private List<Vm> createListOfScalableVms(final int numberOfVms) {
        List<Vm> newList = new ArrayList<>(numberOfVms);
        for (int i = 0; i < numberOfVms; i++) {
            Vm vm = createVm();
            createHorizontalVmScaling(vm);
            newList.add(vm);
        }

        return newList;
    }

    private void createHorizontalVmScaling(Vm vm) {
        HorizontalVmScaling horizontalScaling = new HorizontalVmScalingSimple();
        horizontalScaling
             .setVmSupplier(this::createVm)
             .setOverloadPredicate(this::isVmOverloaded);
        vm.setHorizontalScaling(horizontalScaling);
    }

    private boolean isVmOverloaded(Vm vm) {
        return vm.getCpuPercentUsage() > 0.7;
    }

    /**
     * Creates a Vm object.
     *
     * @return the created Vm
     */
    private Vm createVm() {
        final int id = createsVms++;
        return new VmSimple(id, VM_MIPS, VM_PES)
            .setRam(VM_RAM).setBw(VM_BW).setSize(VM_SIZE)
            .addOnUpdateProcessingListener(this::vmProcessingUpdate)
            .setCloudletScheduler(new CloudletSchedulerTimeShared());
    }

    private Cloudlet createCloudlet() {
        final int id = createdCloudlets++;
        //randomly selects a length for the cloudlet
        final long length = CLOUDLET_LENGTHS[(int) rand.sample()];
        UtilizationModel utilization = new UtilizationModelFull();
        return new CloudletSimple(id, length, 2)
            .setFileSize(1024)
            .setOutputSize(1024)
            .setUtilizationModel(utilization);
        	//.addOnUpdateProcessingListener(this::onUpdateCloudletProcessingListener);
    }
    private void createCloudletsFromWorkloadFile() throws IOException {
        String path = "F:/sarfaraz/cloudSim/Final_code/cloudsim-plus-1.3.1/implemataion/target/classes/workload/swf/";//this.getClass().getClassLoader().getResource("workload/swf").getPath();
        if(path == null)
        {
            path = "";
        }
        String fileName=path + WORKLOAD_FILENAME;
       // String fileName = String.format("%s/%s", path, WORKLOAD_FILENAME);
    //    WorkloadFileReader reader =
        //        new WorkloadFileReader(fileName, CLOUDLETS_MIPS);
      //  reader.setMaxLinesToRead(maximumNumberOfCloudletsToCreateFromTheWorkloadFile);
      //  this.cloudletList = reader.generateWorkload();

    /*    Log.printConcatLine("#Created ", this.cloudletList.size(), " Cloudlets for broker ", broker0.getName());*/
    }
    private Vm bestFitCloudletToVmMapper(final Cloudlet cloudlet) {
       
    	return cloudlet
                .getBroker()
                .getVmCreatedList()
                .stream()
                .filter(vm -> vm.getCpuPercentUsage() <= 0.3 && vm.getCurrentRequestedRam()<100 )
                .min(Comparator.comparingLong(Vm::getNumberOfPes))
                .orElse(Vm.NULL);
        
    }
    private void vmProcessingUpdate(VmHostEventInfo info) {
        final Vm vm = info.getVm();
        //Destroys VM 1 when its CPU usage reaches 90%
        //WriteVmInfo( vm);
        if(vm.getCpuPercentUsage() < 0.3 && vm.isCreated() && vm.getLastBusyTime()>2.0)
        {
        		WriteVmInfo( vm);
            	vm.getHost().destroyVm(vm);
        }
    }
    private void WriteVmInfo(Vm vm)
    {
    	try
    	{
    	
    	  
    	fileWriterVm.append(NEW_LINE_SEPARATOR);

        double cpuusage=vm.getCpuPercentUsage()*100;
    	fileWriterVm.append(String.valueOf(vm.getId() ));
    	fileWriterVm.append(COMMA_DELIMITER);
    	fileWriterVm.append(String.valueOf(cpuusage));
    	fileWriterVm.append(COMMA_DELIMITER);
    	fileWriterVm.append(String.valueOf(vm.getRam()));
    	fileWriterVm.append(COMMA_DELIMITER);
    	fileWriterVm.append(String.valueOf(vm.getBw()));
    	fileWriterVm.append(COMMA_DELIMITER);
    	fileWriterVm.append(String.valueOf(vm.getStorage()));
    	fileWriterVm.append(COMMA_DELIMITER);
    	fileWriterVm.append(String.valueOf(vm.getNumberOfPes()));
    	fileWriterVm.append(COMMA_DELIMITER);
    	fileWriterVm.append(String.valueOf(vm.getStartTime()));
    	fileWriterVm.append(COMMA_DELIMITER);
    	fileWriterVm.append(String.valueOf(vm.getStopTime()));
    	fileWriterVm.append(COMMA_DELIMITER);
    	fileWriterVm.append(String.valueOf(vm.getTotalExecutionTime()));
    	fileWriterVm.append(COMMA_DELIMITER);
            
        

         

    } catch (Exception e) {

        System.out.println("Error in CsvFileWriter !!!");

        e.printStackTrace();

    }
    }
    private void WritCsvFile(String fileName)
    {
    		FileWriter fileWriter = null;
    	  final String FILE_HEADER = "Cloudlet ID,Status ,DC ID,Host ID,Host PEs CPU cores ,VM ID,VM PEs CPU cores  ,CloudletLen MI,CloudletPEs CPU CORE,StartTime SEC,FinishTime SEC,ExecTime SEC";
    		
            try {
 
                fileWriter = new FileWriter(fileName);
  
     
    
                //Write the CSV file header
    
                fileWriter.append(FILE_HEADER.toString());
  
                fileWriter.append(NEW_LINE_SEPARATOR);
  
                for (Cloudlet cloudlet : finishedCloudlets) {
                    fileWriter.append(String.valueOf(cloudlet.getId()));
                    fileWriter.append(COMMA_DELIMITER);
                    fileWriter.append(String.valueOf(cloudlet.getStatus().name()));
                    fileWriter.append(COMMA_DELIMITER);
                    fileWriter.append(String.valueOf(cloudlet.getVm().getHost().getDatacenter().getId()));
                    fileWriter.append(COMMA_DELIMITER);
                    fileWriter.append(String.valueOf(cloudlet.getVm().getHost().getId()));
                    fileWriter.append(COMMA_DELIMITER);
                    fileWriter.append(String.valueOf(cloudlet.getVm().getHost().getWorkingPesNumber()));
                    fileWriter.append(COMMA_DELIMITER);
                    fileWriter.append(String.valueOf(cloudlet.getVm().getId()));
                    fileWriter.append(COMMA_DELIMITER);
                    fileWriter.append(String.valueOf(cloudlet.getVm().getNumberOfPes()));
                    fileWriter.append(COMMA_DELIMITER);
                    fileWriter.append(String.valueOf(cloudlet.getLength()));
                    fileWriter.append(COMMA_DELIMITER);
                    fileWriter.append(String.valueOf(cloudlet.getNumberOfPes()));
                    fileWriter.append(COMMA_DELIMITER);
                    fileWriter.append(String.valueOf(cloudlet.getExecStartTime()));
                    fileWriter.append(COMMA_DELIMITER);
                    fileWriter.append(String.valueOf(cloudlet.getFinishTime()));
                    fileWriter.append(COMMA_DELIMITER);
                    fileWriter.append(String.valueOf(cloudlet.getActualCpuTime()));
                    fileWriter.append(NEW_LINE_SEPARATOR);

                }
  
                System.out.println("CSV file was created successfully !!!");
  
                 
   
            } catch (Exception e) {
 
                System.out.println("Error in CsvFileWriter !!!");
   
                e.printStackTrace();
  
            } finally {

                try {
   
                    fileWriter.flush();
   
                    fileWriter.close();
   
                } catch (IOException e) {
                       System.out.println("Error while flushing/closing fileWriter !!!");
   
                    e.printStackTrace();
                   }
   
                 
           }
    }
 }

